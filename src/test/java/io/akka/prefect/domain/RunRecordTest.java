package io.akka.prefect.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The four sequences named here are the four driven through Prefect's own orchestration in
 * {@code prefect-port/probes/p5_bookkeeping.py}; the field values asserted are the ones it
 * read back.
 */
class RunRecordTest {

  private static final Instant T0 = Instant.parse("2026-08-21T12:00:00Z");

  private static RunState at(StateType type, String name, long seconds) {
    return new RunState(type, name, T0.plusSeconds(seconds), null);
  }

  @Test
  void aFirstStateSetsTheStateFieldsAndNothingElse() {
    var r = RunRecord.initial().record(at(StateType.PENDING, "Pending", 0));
    assertEquals(StateType.PENDING, r.stateType());
    assertEquals("Pending", r.stateName());
    assertEquals(T0, r.stateTimestamp());
    assertNull(r.startTime());
    assertNull(r.endTime());
    assertEquals(0, r.runCount());
    assertEquals(Duration.ZERO, r.totalRunTime());
  }

  @Test
  void enteringRunningSetsTheStartTimeAndCountsTheEntry() {
    var r =
        RunRecord.initial()
            .record(at(StateType.PENDING, "Pending", 0))
            .record(at(StateType.RUNNING, "Running", 1));
    assertEquals(T0.plusSeconds(1), r.startTime());
    assertEquals(1, r.runCount());
  }

  @Test
  void theStartTimeIsSetOnceAndNeverMovedByALaterRunningState() {
    var r =
        RunRecord.initial()
            .record(at(StateType.RUNNING, "Running", 1))
            .record(at(StateType.RUNNING, "Retrying", 4));
    assertEquals(T0.plusSeconds(1), r.startTime());
    assertEquals(2, r.runCount());
  }

  @Test
  void leavingAFinalStateClearsTheEndTime() {
    var r =
        RunRecord.initial()
            .record(at(StateType.PENDING, "Pending", 0))
            .record(at(StateType.RUNNING, "Running", 1))
            .record(at(StateType.FAILED, "Failed", 6));
    assertEquals(T0.plusSeconds(6), r.endTime());
    assertEquals(Duration.ofSeconds(5), r.totalRunTime());

    var revived = r.record(at(StateType.RUNNING, "Running", 10));
    assertNull(revived.endTime());
    assertEquals(T0.plusSeconds(1), revived.startTime());
    assertEquals(Duration.ofSeconds(5), revived.totalRunTime());
    assertEquals(2, revived.runCount());
  }

  @Test
  void finalStateWithoutRunningBackfillsTheStartTime() {
    var r =
        RunRecord.initial()
            .record(at(StateType.PENDING, "Pending", 0))
            .record(at(StateType.FAILED, "Failed", 4));
    assertEquals(T0.plusSeconds(4), r.startTime());
    assertEquals(T0.plusSeconds(4), r.endTime());
    assertEquals(Duration.ZERO, r.totalRunTime());
    assertEquals(0, r.runCount());
  }

  @Test
  void timeAwaitingARetryIsNotCountedAsRunTime() {
    var awaiting = new RunState(StateType.SCHEDULED, "AwaitingRetry", T0.plusSeconds(1), T0.plusSeconds(30));
    var r =
        RunRecord.initial()
            .record(at(StateType.PENDING, "Pending", 0))
            .record(at(StateType.RUNNING, "Running", 0))
            .record(awaiting)
            .record(at(StateType.RUNNING, "Retrying", 31))
            .record(at(StateType.COMPLETED, "Completed", 33));
    assertEquals(Duration.ofSeconds(3), r.totalRunTime());
    assertEquals(Duration.ofSeconds(33), Duration.between(r.startTime(), r.endTime()));
    assertEquals(2, r.runCount());
  }

  @Test
  void theExpectedStartTimeComesFromTheFirstStateAndStaysThere() {
    var scheduled = new RunState(StateType.SCHEDULED, "Scheduled", T0, T0.plusSeconds(60));
    var r = RunRecord.initial().record(scheduled);
    assertEquals(T0.plusSeconds(60), r.expectedStartTime());
    assertEquals(
        T0.plusSeconds(60),
        r.record(at(StateType.RUNNING, "Running", 1)).record(at(StateType.COMPLETED, "Completed", 2)).expectedStartTime());
  }

  @Test
  void aFirstStateThatIsNotScheduledContributesItsOwnTimestamp() {
    assertEquals(T0, RunRecord.initial().record(at(StateType.PENDING, "Pending", 0)).expectedStartTime());
  }

  @Test
  void theNextScheduledStartTimeIsSetOnEntryAndClearedOnExit() {
    var awaiting = new RunState(StateType.SCHEDULED, "AwaitingRetry", T0.plusSeconds(1), T0.plusSeconds(30));
    var scheduled = RunRecord.initial().record(at(StateType.RUNNING, "Running", 0)).record(awaiting);
    assertEquals(T0.plusSeconds(30), scheduled.nextScheduledStartTime());
    assertNull(scheduled.record(at(StateType.RUNNING, "Retrying", 31)).nextScheduledStartTime());
  }

  @Test
  void aScheduledStateDoesNotCountAsAnEntryIntoRunning() {
    var awaiting = new RunState(StateType.SCHEDULED, "AwaitingRetry", T0.plusSeconds(1), T0.plusSeconds(30));
    assertEquals(0, RunRecord.initial().record(awaiting).runCount());
  }

  @Test
  void everyTerminalTypeCountsAsFinal() {
    for (var type : new StateType[] {StateType.COMPLETED, StateType.FAILED, StateType.CANCELLED, StateType.CRASHED}) {
      var r = RunRecord.initial().record(at(StateType.RUNNING, "Running", 0)).record(at(type, type.name(), 2));
      assertEquals(T0.plusSeconds(2), r.endTime(), type.name());
    }
    for (var type : new StateType[] {StateType.SCHEDULED, StateType.PENDING, StateType.RUNNING, StateType.PAUSED, StateType.CANCELLING}) {
      var r = RunRecord.initial().record(at(StateType.RUNNING, "Running", 0)).record(at(type, type.name(), 2));
      assertNull(r.endTime(), type.name());
    }
  }

  @Test
  void recordingTheSameRunningStateTwiceCountsTwice() {
    var r =
        RunRecord.initial()
            .record(at(StateType.RUNNING, "Running", 0))
            .record(at(StateType.RUNNING, "Running", 0));
    assertEquals(2, r.runCount());
  }
}
