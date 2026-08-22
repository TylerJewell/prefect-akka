package io.akka.prefect.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.prefect.domain.CachePolicy;
import io.akka.prefect.domain.JsonValue;
import io.akka.prefect.domain.RetryDecision;
import io.akka.prefect.domain.RetryPolicy;
import io.akka.prefect.domain.RunRecord;
import io.akka.prefect.domain.RunState;
import io.akka.prefect.domain.StateType;
import io.akka.prefect.domain.TaskRunContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The port's side of the benchmark: this rebuild answering every workload in
 * {@code prefect-port/bench/workloads.json}, in the same shape the source side prints.
 *
 * <pre>
 *   java -cp target/classes:&lt;jackson&gt; io.akka.prefect.bench.BenchRunner &lt;workloads.json&gt; answers
 *   java -cp ... io.akka.prefect.bench.BenchRunner &lt;workloads.json&gt; timings
 * </pre>
 */
public final class BenchRunner {

  private static final ObjectMapper JSON = new ObjectMapper();

  private BenchRunner() {}

  public static void main(String[] args) throws IOException {
    var workloads = (ArrayNode) JSON.readTree(Files.readString(Path.of(args[0])));
    var mode = args.length > 1 ? args[1] : "answers";
    if (mode.equals("timings")) {
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(timings(workloads)));
    } else {
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(answers(workloads)));
    }
  }

  // ---------------------------------------------------------------- answers

  private static ObjectNode answers(ArrayNode workloads) {
    var out = JSON.createObjectNode();
    for (var workload : workloads) {
      var name = workload.get("name").asText();
      switch (workload.get("kind").asText()) {
        case "cache-key" -> out.set(name, cacheKeyAnswer(workload));
        case "retry-sequence" -> out.set(name, retryAnswer(workload));
        case "state-sequence" -> out.set(name, stateSequenceAnswer(workload));
        case "arrival-order" -> out.set(name, arrivalOrderAnswer(workload));
        default -> throw new IllegalArgumentException("Unknown workload kind in " + name);
      }
    }
    return out;
  }

  private static ObjectNode cacheKeyAnswer(JsonNode workload) {
    var policy = policyFor(workload);
    var key = policy.computeKey(contextFor(workload), object(workload.get("inputs")), object(workload.get("flowParameters")));
    var answer = JSON.createObjectNode();
    answer.put("key", key);
    var kinds = answer.putArray("effectivePolicies");
    for (var member : members(policy)) {
      kinds.add(member.getClass().getSimpleName());
    }
    return answer;
  }

  /**
   * The source's retry rule is only reachable through its engine, which writes the run's states
   * as it goes; this walks the same loop the engine walks so that the two produce a comparable
   * sequence rather than a comparable single decision.
   */
  private static ObjectNode retryAnswer(JsonNode workload) {
    var delays = new ArrayList<Double>();
    workload.get("delays").forEach(d -> delays.add(d.asDouble()));
    var policy = RetryPolicy.withDelays(workload.get("budget").asInt(), delays);
    boolean retriable = workload.get("retriable").asBoolean();

    var at = Instant.parse("2026-08-21T12:00:00Z");
    var record = RunRecord.initial();
    var states = JSON.createArrayNode();

    record = append(states, record, RunState.of(StateType.PENDING, "Pending", at));
    record = append(states, record, RunState.of(StateType.RUNNING, "Running", at));

    int spent = 0;
    while (true) {
      var decision = policy.decide(spent, retriable, at);
      if (!(decision instanceof RetryDecision.Retry retry)) {
        record = append(states, record, RunState.of(StateType.FAILED, "Failed", at));
        break;
      }
      spent = retry.retriesSpent();
      record = append(states, record, retry.state());
      if (retry.state().type() == StateType.SCHEDULED) {
        record = append(states, record, RunState.of(StateType.RUNNING, "Retrying", at));
      }
    }

    var answer = JSON.createObjectNode();
    answer.set("states", states);
    answer.put("runCount", record.runCount());
    return answer;
  }

  private static RunRecord append(ArrayNode states, RunRecord record, RunState state) {
    var node = JSON.createObjectNode();
    node.put("type", state.type().name());
    node.put("name", state.name());
    if (state.scheduledTime() == null) {
      node.putNull("delaySeconds");
    } else {
      node.put("delaySeconds", Duration.between(state.timestamp(), state.scheduledTime()).toSeconds());
    }
    states.add(node);
    return record.record(state);
  }

  private static ObjectNode stateSequenceAnswer(JsonNode workload) {
    var records = new ArrayList<JsonNode>();
    for (var batch : workload.get("batches")) {
      batch.forEach(records::add);
    }
    return recordSequence(records);
  }

  private static ObjectNode arrivalOrderAnswer(JsonNode workload) {
    var records = new ArrayList<JsonNode>();
    workload.get("records").forEach(records::add);
    var answer = JSON.createObjectNode();
    var perOrder = answer.putArray("perOrder");
    for (int i = 0; i < records.size(); i++) {
      var rotated = new ArrayList<>(records.subList(i, records.size()));
      rotated.addAll(records.subList(0, i));
      perOrder.add(recordSequence(rotated));
    }
    return answer;
  }

  /** Recorded a hundredth of a second apart, the way a caller records them, one after another. */
  private static ObjectNode recordSequence(List<JsonNode> records) {
    var at = Instant.parse("2026-08-21T12:00:00Z");
    var record = RunRecord.initial();
    Instant firstScheduled = null;
    for (int i = 0; i < records.size(); i++) {
      var node = records.get(i);
      var stamp = at.plusMillis(10L * i);
      var type = StateType.valueOf(node.get("type").asText());
      var scheduledAfter = node.get("scheduledAfterSeconds");
      var scheduled = scheduledAfter.isNull() ? null : stamp.plusSeconds(scheduledAfter.asLong());
      if (i == 0) {
        firstScheduled = scheduled;
      }
      record = record.record(new RunState(type, node.get("name").asText(), stamp, scheduled));
    }
    var answer = JSON.createObjectNode();
    answer.put("stateType", record.stateType().name());
    answer.put("stateName", record.stateName());
    answer.put("runCount", record.runCount());
    answer.put("statesRecorded", records.size());
    answer.put("startTimeSet", record.startTime() != null);
    answer.put("endTimeSet", record.endTime() != null);
    answer.put("nextScheduledSet", record.nextScheduledStartTime() != null);
    answer.put(
        "expectedStartIsTheScheduledTime",
        firstScheduled != null && firstScheduled.equals(record.expectedStartTime()));
    answer.put("runTimeAccrued", record.totalRunTime().compareTo(Duration.ZERO) > 0);
    answer.put(
        "startTimeIsTheEndTime",
        record.startTime() != null && record.startTime().equals(record.endTime()));
    return answer;
  }

  // ---------------------------------------------------------------- timings

  /**
   * Each figure is a window's total divided by what was in it, and the window is sized from a
   * pilot so that it runs for tens of milliseconds — a repetition short enough to be quantised by
   * the platform clock reports the clock rather than the work.
   */
  private static ObjectNode timings(ArrayNode workloads) {
    var out = JSON.createObjectNode();
    for (var workload : workloads) {
      if (!workload.get("kind").asText().equals("cache-key")) {
        continue;
      }
      var policy = policyFor(workload);
      var context = contextFor(workload);
      var inputs = object(workload.get("inputs"));
      var parameters = object(workload.get("flowParameters"));
      Runnable once = () -> policy.computeKey(context, inputs, parameters);

      for (int i = 0; i < 50_000; i++) {
        once.run();
      }
      long pilot = time(once, 1_000) / 1_000;
      int perWindow = (int) Math.max(1_000, Math.min(2_000_000, 30_000_000L / Math.max(pilot, 1)));

      var nanos = new ArrayList<Long>();
      for (int window = 0; window < 9; window++) {
        nanos.add(time(once, perWindow) / perWindow);
      }
      nanos.sort(Long::compareTo);
      var figure = out.putObject(workload.get("name").asText());
      figure.put("nanosPerKey", nanos.get(nanos.size() / 2));
      figure.put("perWindow", perWindow);
      figure.put("windows", nanos.size());
    }
    return out;
  }

  private static long time(Runnable once, int repetitions) {
    long started = System.nanoTime();
    for (int i = 0; i < repetitions; i++) {
      once.run();
    }
    return System.nanoTime() - started;
  }

  // ---------------------------------------------------------------- shared

  private static CachePolicy policyFor(JsonNode workload) {
    var specs = workload.get("policies");
    if (specs == null || specs.isEmpty()) {
      return CachePolicy.DEFAULT;
    }
    var members = new ArrayList<CachePolicy>();
    for (var spec : specs) {
      var exclude = new ArrayList<String>();
      if (spec.hasNonNull("exclude")) {
        spec.get("exclude").forEach(name -> exclude.add(name.asText()));
      }
      members.add(
          switch (spec.get("kind").asText()) {
            case "inputs" -> CachePolicy.inputs(exclude.toArray(String[]::new));
            case "task_source" -> CachePolicy.TASK_SOURCE;
            case "flow_parameters" -> CachePolicy.FLOW_PARAMETERS;
            case "run_id" -> CachePolicy.RUN_ID;
            case "no_cache" -> CachePolicy.NO_CACHE;
            default -> throw new IllegalArgumentException("Unknown policy " + spec.get("kind"));
          });
    }
    return CachePolicy.compound(members.toArray(CachePolicy[]::new));
  }

  private static List<CachePolicy> members(CachePolicy policy) {
    return policy instanceof CachePolicy.Compound compound ? compound.policies() : List.of(policy);
  }

  private static TaskRunContext contextFor(JsonNode workload) {
    return new TaskRunContext(
        workload.get("taskRunId").asText(),
        workload.get("flowRunId").asText(),
        workload.get("sourceText").asText());
  }

  private static JsonValue.JObj object(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    var fields = new LinkedHashMap<String, JsonValue>();
    node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), value(entry.getValue())));
    return JsonValue.obj(fields);
  }

  private static JsonValue value(JsonNode node) {
    if (node.isNull()) {
      return JsonValue.NULL;
    }
    if (node.isTextual()) {
      return JsonValue.str(node.asText());
    }
    if (node.isBoolean()) {
      return JsonValue.bool(node.asBoolean());
    }
    if (node.isIntegralNumber()) {
      return JsonValue.num(node.asLong());
    }
    if (node.isNumber()) {
      return JsonValue.num(node.asDouble());
    }
    if (node.isArray()) {
      var items = new ArrayList<JsonValue>();
      node.forEach(item -> items.add(value(item)));
      return JsonValue.arr(items);
    }
    var fields = new LinkedHashMap<String, JsonValue>();
    node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), value(entry.getValue())));
    return JsonValue.obj(Map.copyOf(fields));
  }
}
