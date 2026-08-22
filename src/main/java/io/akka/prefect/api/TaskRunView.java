package io.akka.prefect.api;

import io.akka.prefect.domain.RunRecord;
import io.akka.prefect.domain.RunState;
import io.akka.prefect.domain.TaskRunState;
import java.util.List;

/** What a caller sees of a task run. */
public record TaskRunView(
    RunRecord record, int retriesSpent, int statesRecorded, List<RunState> history) {

  static TaskRunView from(TaskRunState state) {
    return new TaskRunView(
        state.record(), state.retriesSpent(), state.statesRecorded(), state.history());
  }
}
