package io.akka.prefect.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.http.HttpException;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import io.akka.prefect.domain.CachePolicy;
import io.akka.prefect.domain.JsonValue;
import io.akka.prefect.domain.TaskRunContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Computing a cache key needs no run and keeps no state, so it is its own surface. */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/cache-keys")
public class CacheKeyEndpoint {

  /** One policy by name, with the argument names an {@code inputs} policy leaves out. */
  public record PolicySpec(String kind, List<String> exclude) {}

  public record ComputeRequest(
      List<PolicySpec> policies,
      Map<String, Object> inputs,
      Map<String, Object> flowParameters,
      String taskRunId,
      String flowRunId,
      String sourceText) {}

  /** The key, and the policies in the order they will actually be consulted. */
  public record ComputeResponse(String key, List<String> effectivePolicies) {}

  @Post("/compute")
  public ComputeResponse compute(ComputeRequest request) {
    try {
      var policy = toPolicy(request.policies());
      var context = new TaskRunContext(request.taskRunId(), request.flowRunId(), request.sourceText());
      var key = policy.computeKey(context, toObject(request.inputs()), toObject(request.flowParameters()));
      return new ComputeResponse(key, effectiveKinds(policy));
    } catch (IllegalArgumentException rejected) {
      throw HttpException.badRequest(rejected.getMessage());
    }
  }

  private static CachePolicy toPolicy(List<PolicySpec> specs) {
    if (specs == null || specs.isEmpty()) {
      return CachePolicy.DEFAULT;
    }
    var members = new ArrayList<CachePolicy>();
    for (var spec : specs) {
      members.add(named(spec));
    }
    return CachePolicy.compound(members.toArray(CachePolicy[]::new));
  }

  private static CachePolicy named(PolicySpec spec) {
    var exclude = spec.exclude() == null ? List.<String>of() : spec.exclude();
    return switch (spec.kind()) {
      case "inputs" -> CachePolicy.inputs(exclude.toArray(String[]::new));
      case "task_source" -> CachePolicy.TASK_SOURCE;
      case "flow_parameters" -> CachePolicy.FLOW_PARAMETERS;
      case "run_id" -> CachePolicy.RUN_ID;
      case "no_cache" -> CachePolicy.NO_CACHE;
      default -> throw new IllegalArgumentException(
          "Unknown cache policy. Known policies are: inputs, task_source, flow_parameters, run_id, no_cache.");
    };
  }

  private static List<String> effectiveKinds(CachePolicy policy) {
    if (policy instanceof CachePolicy.Compound compound) {
      return compound.policies().stream().map(p -> p.getClass().getSimpleName()).toList();
    }
    return List.of(policy.getClass().getSimpleName());
  }

  private static JsonValue.JObj toObject(Map<String, Object> raw) {
    if (raw == null) {
      return null;
    }
    return (JsonValue.JObj) JsonValue.of(raw);
  }
}
