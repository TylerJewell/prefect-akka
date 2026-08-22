package io.akka.prefect.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * What a run carries about itself as its states are recorded. Every field here is derived from the
 * sequence of states, never set directly, so a record and its state history cannot disagree.
 *
 * <p>The rules in {@link #record} all apply on every recording, in the order written, each reading
 * the state being left before the state being recorded replaces it.
 */
public record RunRecord(
    StateType stateType,
    String stateName,
    Instant stateTimestamp,
    Instant startTime,
    Instant endTime,
    Instant expectedStartTime,
    Instant nextScheduledStartTime,
    Duration totalRunTime,
    int runCount) {

  public static RunRecord initial() {
    return new RunRecord(null, null, null, null, null, null, null, Duration.ZERO, 0);
  }

  public RunRecord record(RunState proposed) {
    var leaving = stateType;
    var leavingTimestamp = stateTimestamp;

    var newStartTime = startTime;
    var newEndTime = endTime;
    var newTotalRunTime = totalRunTime;
    var newRunCount = runCount;
    var newExpectedStartTime = expectedStartTime;
    var newNextScheduled = nextScheduledStartTime;

    if (proposed.type() == StateType.RUNNING && newStartTime == null) {
      newStartTime = proposed.timestamp();
    }

    if (leaving != null && leaving.isFinal() && !proposed.type().isFinal()) {
      newEndTime = null;
    }
    if (proposed.type().isFinal() && newEndTime == null) {
      if (newStartTime == null) {
        newStartTime = proposed.timestamp();
      }
      newEndTime = proposed.timestamp();
    }

    if (leaving == StateType.RUNNING) {
      newTotalRunTime = newTotalRunTime.plus(Duration.between(leavingTimestamp, proposed.timestamp()));
    }

    if (proposed.type() == StateType.RUNNING) {
      newRunCount = newRunCount + 1;
    }

    if (newExpectedStartTime == null) {
      newExpectedStartTime =
          proposed.type() == StateType.SCHEDULED ? proposed.scheduledTime() : proposed.timestamp();
    }

    if (leaving == StateType.SCHEDULED) {
      newNextScheduled = null;
    }
    if (proposed.type() == StateType.SCHEDULED) {
      newNextScheduled = proposed.scheduledTime();
    }

    return new RunRecord(
        proposed.type(),
        proposed.name(),
        proposed.timestamp(),
        newStartTime,
        newEndTime,
        newExpectedStartTime,
        newNextScheduled,
        newTotalRunTime,
        newRunCount);
  }
}
