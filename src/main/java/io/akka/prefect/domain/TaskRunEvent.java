package io.akka.prefect.domain;

import akka.javasdk.annotations.TypeName;
import java.time.Instant;
import java.util.List;

/** What a task run's journal holds. A run's whole record is a fold over these. */
public sealed interface TaskRunEvent {

  @TypeName("state-recorded")
  record StateRecorded(StateType type, String name, Instant timestamp, Instant scheduledTime)
      implements TaskRunEvent {

    public RunState toState() {
      return new RunState(type, name, timestamp, scheduledTime);
    }

    public static StateRecorded from(RunState state) {
      return new StateRecorded(state.type(), state.name(), state.timestamp(), state.scheduledTime());
    }
  }

  @TypeName("retry-budget-set")
  record RetryBudgetSet(int budget, List<Double> delays, double jitterFactor) implements TaskRunEvent {}

  /** One granted retry. Written alongside the state it grants, so the count is not inferred from names. */
  @TypeName("retry-granted")
  record RetryGranted(int retriesSpent) implements TaskRunEvent {}
}
