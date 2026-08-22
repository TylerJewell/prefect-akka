package io.akka.prefect.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/** The bound on retained history, and the count that is not bounded. */
class TaskRunStateTest {

  private static final Instant T0 = Instant.parse("2026-08-21T12:00:00Z");

  private static TaskRunState afterRecording(int count) {
    var state = TaskRunState.empty();
    for (int i = 0; i < count; i++) {
      state =
          state.apply(
              new TaskRunEvent.StateRecorded(StateType.RUNNING, "Running #" + i, T0.plusSeconds(i), null));
    }
    return state;
  }

  @Test
  void historyKeepsTheMostRecentStatesAndDropsTheOldest() {
    var state = afterRecording(TaskRunState.HISTORY_LIMIT + 5);
    assertThat(state.history()).hasSize(TaskRunState.HISTORY_LIMIT);
    assertThat(state.history().get(0).name()).isEqualTo("Running #5");
    assertThat(state.history().get(TaskRunState.HISTORY_LIMIT - 1).name())
        .isEqualTo("Running #" + (TaskRunState.HISTORY_LIMIT + 4));
  }

  @Test
  void theCountOfRecordedStatesIsNotBounded() {
    assertThat(afterRecording(TaskRunState.HISTORY_LIMIT + 5).statesRecorded())
        .isEqualTo(TaskRunState.HISTORY_LIMIT + 5);
  }

  @Test
  void theBookkeepingDoesNotDependOnWhatHistoryWasDropped() {
    var state = afterRecording(TaskRunState.HISTORY_LIMIT + 5);
    assertThat(state.record().runCount()).isEqualTo(TaskRunState.HISTORY_LIMIT + 5);
    assertThat(state.record().startTime()).isEqualTo(T0);
  }
}
