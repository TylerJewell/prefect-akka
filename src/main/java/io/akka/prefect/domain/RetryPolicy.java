package io.akka.prefect.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The retry budget attached to a task, and the decision it makes about one failed attempt.
 *
 * <p>An empty delay list means the retry happens immediately, into a running state; any delay at
 * all, including zero, means it happens into a scheduled state. A jitter factor is carried but
 * spends nothing unless a caller asks for it with a uniform draw, which is what
 * {@link #delayForAttempt(int, double)} is for — the local path Prefect's own engine takes never
 * asks either.
 */
public record RetryPolicy(int budget, List<Double> delays, double jitterFactor) {

  public RetryPolicy {
    delays = List.copyOf(delays);
  }

  public static RetryPolicy of(int budget) {
    return new RetryPolicy(budget, List.of(), 0.0);
  }

  public static RetryPolicy withDelays(int budget, List<Double> delays) {
    return new RetryPolicy(budget, delays, 0.0);
  }

  public RetryPolicy withJitter(double jitterFactor) {
    return new RetryPolicy(budget, delays, jitterFactor);
  }

  public RetryDecision decide(int retriesSpent, boolean retriable, Instant at) {
    return decide(retriesSpent, retriable, at, NO_JITTER);
  }

  public RetryDecision decide(int retriesSpent, boolean retriable, Instant at, double uniformDraw) {
    if (retriesSpent >= budget) {
      return new RetryDecision.Exhausted();
    }
    if (!retriable) {
      return new RetryDecision.NotRetriable();
    }
    if (delays.isEmpty()) {
      return new RetryDecision.Retry(
          RunState.of(StateType.RUNNING, "Retrying", at), retriesSpent + 1, 0.0);
    }
    double delay = delayForAttempt(retriesSpent, uniformDraw);
    var scheduled = at.plus(Duration.ofNanos(Math.round(delay * 1_000_000_000L)));
    return new RetryDecision.Retry(
        new RunState(StateType.SCHEDULED, "AwaitingRetry", at, scheduled), retriesSpent + 1, delay);
  }

  /** The base delay for an attempt: index {@code min(retriesSpent, size - 1)}, so the last repeats. */
  public double baseDelayForAttempt(int retriesSpent) {
    if (delays.isEmpty()) {
      return 0.0;
    }
    return delays.get(Math.min(retriesSpent, delays.size() - 1));
  }

  /**
   * The delay an attempt waits. A {@code uniformDraw} outside (0, 1) means no jitter was asked for
   * and the base delay stands; a draw inside it spreads the delay over the clamped exponential
   * distribution Prefect's server rule uses.
   */
  public double delayForAttempt(int retriesSpent, double uniformDraw) {
    double base = baseDelayForAttempt(retriesSpent);
    if (jitterFactor <= 0 || base <= 0 || uniformDraw <= 0 || uniformDraw >= 1) {
      return base;
    }
    return clampedPoissonInterval(base, jitterFactor, uniformDraw);
  }

  private static final double NO_JITTER = -1.0;

  static double clampedPoissonInterval(double averageInterval, double clampingFactor, double uniformDraw) {
    double upperClampMultiple = 1 + clampingFactor;
    double upperBound = averageInterval * upperClampMultiple;
    double lowerBound = Math.max(0, averageInterval * lowerClampMultiple(upperClampMultiple));
    double upper = exponentialCdf(upperBound, averageInterval);
    double lower = exponentialCdf(lowerBound, averageInterval);
    double drawn = lower + (upper - lower) * uniformDraw;
    return -Math.log(Math.max(1 - drawn, 1e-10)) * averageInterval;
  }

  static double lowerClampMultiple(double k) {
    if (k >= 50) {
      return 0.0;
    }
    return Math.log(Math.max(Math.pow(2, k) / (Math.pow(2, k) - 1), 1e-10)) / Math.log(2);
  }

  static double exponentialCdf(double x, double averageInterval) {
    return 1 - Math.exp(-x / averageInterval);
  }
}
