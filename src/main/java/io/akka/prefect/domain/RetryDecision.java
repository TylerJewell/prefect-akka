package io.akka.prefect.domain;

/** What a failed attempt is allowed to do next. */
public sealed interface RetryDecision {

  /** The attempt retries, into {@code state}, and the retries spent become {@code retriesSpent}. */
  record Retry(RunState state, int retriesSpent, double delaySeconds) implements RetryDecision {}

  /** The retry budget is spent. */
  record Exhausted(String reason) implements RetryDecision {
    public Exhausted() {
      this("Retries are exhausted");
    }
  }

  /** Retries remain, but the condition attached to them answered no. */
  record NotRetriable(String reason) implements RetryDecision {
    public NotRetriable() {
      this("The retry condition answered no");
    }
  }
}
