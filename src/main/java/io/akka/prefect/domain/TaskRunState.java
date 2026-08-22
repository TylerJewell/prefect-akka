package io.akka.prefect.domain;



import java.util.ArrayList;

import java.util.List;



/**

 * A task run as its journal leaves it: the record maintained by SPEC-001 section 3, the retry budget it

 * was given, how many retries that budget has spent, and the states it recorded.

 *

 * <p>{@code history} keeps only the most recent {@link #HISTORY_LIMIT} states while

 * {@code statesRecorded} counts all of them. Entity state has to replicate under a size ceiling,

 * so a list that only ever appends would eventually stop replicating; the count is what the

 * bookkeeping in {@link RunRecord} is derived from and it is never truncated.

 */

public record TaskRunState(

    RunRecord record,

    RetryPolicy retryPolicy,

    int retriesSpent,

    int statesRecorded,

    List<RunState> history) {



  /** Two orders of magnitude above the longest retry budget Prefect's backoff helper can configure. */

  public static final int HISTORY_LIMIT = 1000;



  public static TaskRunState empty() {

    return new TaskRunState(RunRecord.initial(), RetryPolicy.of(0), 0, 0, List.of());

  }



  public TaskRunState apply(TaskRunEvent event) {

    return switch (event) {

      case TaskRunEvent.StateRecorded recorded -> {

        var state = recorded.toState();

        var grown = new ArrayList<>(history);

        grown.add(state);

        if (grown.size() > HISTORY_LIMIT) {

          grown.subList(0, grown.size() - HISTORY_LIMIT).clear();

        }

        yield new TaskRunState(

            record.record(state), retryPolicy, retriesSpent, statesRecorded + 1, List.copyOf(grown));

      }

      case TaskRunEvent.RetryBudgetSet set ->

          new TaskRunState(

              record,

              new RetryPolicy(set.budget(), set.delays(), set.jitterFactor()),

              retriesSpent,

              statesRecorded,

              history);

      case TaskRunEvent.RetryGranted granted ->

          new TaskRunState(record, retryPolicy, granted.retriesSpent(), statesRecorded, history);

    };

  }

}

