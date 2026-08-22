package io.akka.prefect.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.prefect.domain.StateType;
import io.akka.prefect.domain.TaskRunEvent;
import io.akka.prefect.domain.TaskRunState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * What the domain tests cannot check: that a run driven through the entity writes the same state
 * sequence Prefect writes, and that the timestamp on a recorded state is the entity's and not the
 * caller's.
 */
public class TaskRunEntityTest {

  private static final Instant T0 = Instant.parse("2026-08-21T12:00:00Z");

  private final AtomicReference<Instant> clock = new AtomicReference<>(T0);

  private EventSourcedTestKit<TaskRunState, TaskRunEvent, TaskRunEntity> run() {
    return EventSourcedTestKit.of(
        "task-run-1",
        ctx -> {
          return new TaskRunEntity(ctx) {
            @Override
            protected Instant now() {
              return clock.get();
            }
          };
        });
  }

  private void advance(long seconds) {
    clock.updateAndGet(t -> t.plusSeconds(seconds));
  }

  private static List<String> names(TaskRunState state) {
    return state.history().stream().map(s -> s.type() + "/" + s.name()).toList();
  }

  @Test
  public void recordingStampsTheStateAtTheBoundary() {
    var kit = run();
    advance(5);
    var result =
        kit.method(TaskRunEntity::recordState)
            .invoke(new TaskRunEntity.RecordState(StateType.PENDING, "Pending", null));
    assertThat(result.getReply().record().stateTimestamp()).isEqualTo(T0.plusSeconds(5));
  }

  @Test
  public void aCallerSuppliedScheduledTimeIsKeptEvenThoughTheTimestampIsNot() {
    var kit = run();
    var scheduled = T0.plusSeconds(90);
    var result =
        kit.method(TaskRunEntity::recordState)
            .invoke(new TaskRunEntity.RecordState(StateType.SCHEDULED, "Scheduled", scheduled));
    assertThat(result.getReply().record().nextScheduledStartTime()).isEqualTo(scheduled);
    assertThat(result.getReply().record().expectedStartTime()).isEqualTo(scheduled);
  }

  @Test
  public void aRunThatSucceedsFirstTimeWritesPendingRunningCompleted() {
    var kit = run();
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.PENDING, "Pending", null));
    advance(1);
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    advance(2);
    var last =
        kit.method(TaskRunEntity::recordState)
            .invoke(new TaskRunEntity.RecordState(StateType.COMPLETED, "Completed", null));
    assertThat(names(last.getReply()))
        .containsExactly("PENDING/Pending", "RUNNING/Running", "COMPLETED/Completed");
    assertThat(last.getReply().record().runCount()).isEqualTo(1);
    assertThat(last.getReply().record().totalRunTime()).isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  public void aRetryingRunWritesTheSameStateSequenceAsPrefect() {
    var kit = run();
    kit.method(TaskRunEntity::setRetryBudget)
        .invoke(new TaskRunEntity.SetRetryBudget(2, List.of(), 0.0));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.PENDING, "Pending", null));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));
    var last =
        kit.method(TaskRunEntity::recordState)
            .invoke(new TaskRunEntity.RecordState(StateType.COMPLETED, "Completed", null));

    assertThat(names(last.getReply()))
        .containsExactly("PENDING/Pending", "RUNNING/Running", "RUNNING/Retrying", "COMPLETED/Completed");
    assertThat(last.getReply().record().runCount()).isEqualTo(2);
    assertThat(last.getReply().retriesSpent()).isEqualTo(1);
  }

  @Test
  public void aDelayedRetryWritesAwaitingRetryBeforeRetrying() {
    var kit = run();
    kit.method(TaskRunEntity::setRetryBudget)
        .invoke(new TaskRunEntity.SetRetryBudget(2, List.of(30.0), 0.0));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    var afterFailure = kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));

    assertThat(names(afterFailure.getReply())).containsExactly("RUNNING/Running", "SCHEDULED/AwaitingRetry");
    assertThat(afterFailure.getReply().record().nextScheduledStartTime()).isEqualTo(T0.plusSeconds(30));
    assertThat(afterFailure.getReply().record().endTime()).isNull();
  }

  @Test
  public void aFailureWithNoBudgetLeftIsRecordedAsFailed() {
    var kit = run();
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    advance(3);
    var result = kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));

    assertThat(names(result.getReply())).containsExactly("RUNNING/Running", "FAILED/Failed");
    assertThat(result.getReply().record().endTime()).isEqualTo(T0.plusSeconds(3));
    assertThat(result.getReply().retriesSpent()).isEqualTo(0);
  }

  @Test
  public void aFailureTheConditionRefusesIsRecordedAsFailedWithTheBudgetUnspent() {
    var kit = run();
    kit.method(TaskRunEntity::setRetryBudget).invoke(new TaskRunEntity.SetRetryBudget(3, List.of(), 0.0));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    var result = kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", false));

    assertThat(names(result.getReply())).containsExactly("RUNNING/Running", "FAILED/Failed");
    assertThat(result.getReply().retriesSpent()).isEqualTo(0);
  }

  @Test
  public void aBudgetOfTwoFailingEveryTimeRunsThreeTimes() {
    var kit = run();
    kit.method(TaskRunEntity::setRetryBudget).invoke(new TaskRunEntity.SetRetryBudget(2, List.of(), 0.0));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));
    kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));
    var last = kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));

    assertThat(names(last.getReply()))
        .containsExactly("RUNNING/Running", "RUNNING/Retrying", "RUNNING/Retrying", "FAILED/Failed");
    assertThat(last.getReply().record().runCount()).isEqualTo(3);
  }

  @Test
  public void whereTheBatchBoundariesFallChangesNothing() {
    var states =
        List.of(
            new TaskRunEntity.RecordState(StateType.PENDING, "Pending", null),
            new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null),
            new TaskRunEntity.RecordState(StateType.RUNNING, "Retrying", null),
            new TaskRunEntity.RecordState(StateType.COMPLETED, "Completed", null));

    var whole = run();
    whole.method(TaskRunEntity::recordStates).invoke(states);

    var cut = run();
    cut.method(TaskRunEntity::recordStates).invoke(states.subList(0, 1));
    cut.method(TaskRunEntity::recordStates).invoke(states.subList(1, 3));
    cut.method(TaskRunEntity::recordStates).invoke(states.subList(3, 4));

    assertThat(cut.getState()).isEqualTo(whole.getState());
  }

  @Test
  public void anEmptyBatchRecordsNothing() {
    var kit = run();
    var result = kit.method(TaskRunEntity::recordStates).invoke(List.of());
    assertThat(result.didPersistEvents()).isFalse();
    assertThat(result.getReply().statesRecorded()).isZero();
  }

  @Test
  public void theRecordSurvivesAReplayOfTheJournal() {
    var kit = run();
    kit.method(TaskRunEntity::setRetryBudget).invoke(new TaskRunEntity.SetRetryBudget(1, List.of(), 0.0));
    kit.method(TaskRunEntity::recordState).invoke(new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    advance(4);
    kit.method(TaskRunEntity::reportFailure).invoke(new TaskRunEntity.ReportFailure("boom", true));
    var before = kit.getState();

    var replayed = TaskRunState.empty();
    for (var event : kit.getAllEvents()) {
      replayed = replayed.apply((TaskRunEvent) event);
    }
    assertThat(replayed).isEqualTo(before);
  }
}
