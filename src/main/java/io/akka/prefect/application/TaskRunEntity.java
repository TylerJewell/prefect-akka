package io.akka.prefect.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.prefect.domain.RetryDecision;
import io.akka.prefect.domain.RunState;
import io.akka.prefect.domain.StateType;
import io.akka.prefect.domain.TaskRunEvent;
import io.akka.prefect.domain.TaskRunState;
import java.time.Instant;
import java.util.List;

/**
 * One task run — SPEC-001. The entity id is the task run id. Every rule about what a state
 * recording does to the run's fields lives in {@code io.akka.prefect.domain} and is tested without
 * a runtime; this class decides what to persist and supplies the clock, which a fold over the
 * journal must not read.
 */
@Component(id = "task-run")
public class TaskRunEntity extends EventSourcedEntity<TaskRunState, TaskRunEvent> {

  public TaskRunEntity(EventSourcedEntityContext context) {}

  /** The moment a state is recorded at. The runtime requires an entity to declare exactly one
   * constructor, so a test that needs a fixed clock overrides this rather than injecting one. */
  protected Instant now() {
    return Instant.now();
  }

  @Override
  public TaskRunState emptyState() {
    return TaskRunState.empty();
  }

  @Override
  public TaskRunState applyEvent(TaskRunEvent event) {
    return currentState().apply(event);
  }

  public record SetRetryBudget(int budget, List<Double> delays, double jitterFactor) {}

  public Effect<Done> setRetryBudget(SetRetryBudget budget) {
    return effects()
        .persist(
            new TaskRunEvent.RetryBudgetSet(
                budget.budget(),
                budget.delays() == null ? List.of() : budget.delays(),
                budget.jitterFactor()))
        .thenReply(state -> Done.getInstance());
  }

  /**
   * The state to record. Its timestamp is assigned here and a caller-supplied one is ignored, which
   * is what Prefect's server does; a scheduled time, by contrast, is the caller's to choose.
   */
  public record RecordState(StateType type, String name, Instant scheduledTime) {}

  public Effect<TaskRunState> recordState(RecordState command) {
    var state = new RunState(command.type(), command.name(), now(), command.scheduledTime());
    return effects()
        .persist(TaskRunEvent.StateRecorded.from(state))
        .thenReply(updated -> updated);
  }

  /**
   * Several states recorded in one call. Each is stamped and folded in turn, so where the batch
   * boundaries fall changes nothing about the resulting record — the boundaries are a property of
   * how the states were delivered, not of the run.
   */
  public Effect<TaskRunState> recordStates(List<RecordState> commands) {
    var events = new java.util.ArrayList<TaskRunEvent>(commands.size());
    for (var command : commands) {
      events.add(
          TaskRunEvent.StateRecorded.from(
              new RunState(command.type(), command.name(), now(), command.scheduledTime())));
    }
    if (events.isEmpty()) {
      return effects().reply(currentState());
    }
    return effects().persistAll(events).thenReply(updated -> updated);
  }

  /** Whether the run's retry condition allows another attempt; Prefect calls this the retry condition. */
  public record ReportFailure(String message, boolean retriable) {}

  /**
   * Records the outcome of a failed attempt: either the retry state the budget allows, or the
   * failed state it does not.
   */
  public Effect<TaskRunState> reportFailure(ReportFailure report) {
    var now = now();
    var decision = currentState().retryPolicy().decide(currentState().retriesSpent(), report.retriable(), now);
    if (decision instanceof RetryDecision.Retry retry) {
      return effects()
          .persistAll(
              List.of(
                  new TaskRunEvent.RetryGranted(retry.retriesSpent()),
                  TaskRunEvent.StateRecorded.from(retry.state())))
          .thenReply(updated -> updated);
    }
    var failed = RunState.of(StateType.FAILED, "Failed", now);
    return effects().persist(TaskRunEvent.StateRecorded.from(failed)).thenReply(updated -> updated);
  }

  public ReadOnlyEffect<TaskRunState> get() {
    return effects().reply(currentState());
  }
}
