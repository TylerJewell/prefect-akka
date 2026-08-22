package io.akka.prefect.domain;

import java.util.ArrayList;
import java.util.List;

/** The delay list an exponentially backing-off retry budget uses. */
public final class Backoff {

  /** No more than fifty delays can be configured on a task. */
  private static final int MAX_DELAYS = 50;

  private Backoff() {}

  public static List<Double> exponential(double factor, int retries) {
    int steps = Math.min(retries, MAX_DELAYS);
    var delays = new ArrayList<Double>(Math.max(steps, 0));
    for (int r = 0; r < steps; r++) {
      delays.add(factor * Math.pow(2, r));
    }
    return List.copyOf(delays);
  }
}
