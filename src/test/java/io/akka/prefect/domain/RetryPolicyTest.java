package io.akka.prefect.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  private static final Instant T0 = Instant.parse("2026-08-21T12:00:00Z");

  @Test
  void aBudgetOfNAllowsNRetriesAndThenRefuses() {
    var policy = RetryPolicy.of(2);
    assertInstanceOf(RetryDecision.Retry.class, policy.decide(0, true, T0));
    assertInstanceOf(RetryDecision.Retry.class, policy.decide(1, true, T0));
    assertInstanceOf(RetryDecision.Exhausted.class, policy.decide(2, true, T0));
    assertInstanceOf(RetryDecision.Exhausted.class, policy.decide(3, true, T0));
  }

  @Test
  void aBudgetOfZeroNeverRetries() {
    assertInstanceOf(RetryDecision.Exhausted.class, RetryPolicy.of(0).decide(0, true, T0));
  }

  @Test
  void aConditionThatAnswersNoStopsARetryThatWasStillAffordable() {
    var refused = RetryPolicy.of(2).decide(0, false, T0);
    assertInstanceOf(RetryDecision.NotRetriable.class, refused);
  }

  @Test
  void aRetryWithNoDelayIsRunningAndNamedRetrying() {
    var retry = (RetryDecision.Retry) RetryPolicy.of(1).decide(0, true, T0);
    assertEquals(StateType.RUNNING, retry.state().type());
    assertEquals("Retrying", retry.state().name());
    assertEquals(null, retry.state().scheduledTime());
    assertEquals(1, retry.retriesSpent());
  }

  @Test
  void aRetryWithADelayIsScheduledForThatManySecondsAhead() {
    var retry = (RetryDecision.Retry) RetryPolicy.withDelays(3, List.of(30.0)).decide(0, true, T0);
    assertEquals(StateType.SCHEDULED, retry.state().type());
    assertEquals("AwaitingRetry", retry.state().name());
    assertEquals(T0.plusSeconds(30), retry.state().scheduledTime());
  }

  @Test
  void aDelayOfZeroIsStillADelayAndStillSchedules() {
    var retry = (RetryDecision.Retry) RetryPolicy.withDelays(1, List.of(0.0)).decide(0, true, T0);
    assertEquals(StateType.SCHEDULED, retry.state().type());
    assertEquals(T0, retry.state().scheduledTime());
  }

  @Test
  void eachAttemptTakesTheDelayAtItsOwnIndex() {
    var policy = RetryPolicy.withDelays(3, List.of(7.0, 13.0, 29.0));
    assertEquals(T0.plusSeconds(7), scheduledFor(policy, 0));
    assertEquals(T0.plusSeconds(13), scheduledFor(policy, 1));
    assertEquals(T0.plusSeconds(29), scheduledFor(policy, 2));
  }

  @Test
  void finalDelayRepeatsPastTheEndOfTheList() {
    var policy = RetryPolicy.withDelays(5, List.of(1.0, 2.0));
    assertEquals(
        List.of(
            T0.plusSeconds(1),
            T0.plusSeconds(2),
            T0.plusSeconds(2),
            T0.plusSeconds(2),
            T0.plusSeconds(2)),
        List.of(
            scheduledFor(policy, 0),
            scheduledFor(policy, 1),
            scheduledFor(policy, 2),
            scheduledFor(policy, 3),
            scheduledFor(policy, 4)));
  }

  @Test
  void aSingleDelayAppliesToEveryAttempt() {
    var policy = RetryPolicy.withDelays(3, List.of(5.0));
    assertEquals(T0.plusSeconds(5), scheduledFor(policy, 0));
    assertEquals(T0.plusSeconds(5), scheduledFor(policy, 2));
  }

  @Test
  void jitterLeavesTheDelayAloneByDefault() {
    var policy = RetryPolicy.withDelays(3, List.of(2.0)).withJitter(1.0);
    assertEquals(T0.plusSeconds(2), scheduledFor(policy, 0));
    assertEquals(T0.plusSeconds(2), scheduledFor(policy, 1));
  }

  @Test
  void jitterAskedForExplicitlyStaysInsideItsClamp() {
    var policy = RetryPolicy.withDelays(3, List.of(10.0)).withJitter(0.3);
    for (double u = 0.001; u < 1.0; u += 0.05) {
      double delay = policy.delayForAttempt(0, u);
      assertTrue(delay <= 10.0 * 1.3 + 1e-9, "upper clamp: " + delay);
      assertTrue(delay >= 0.0, "lower clamp: " + delay);
    }
  }

  @Test
  void jitterIsNotAskedForWhenTheBaseDelayIsZero() {
    var policy = RetryPolicy.withDelays(1, List.of(0.0)).withJitter(0.3);
    assertEquals(0.0, policy.delayForAttempt(0, 0.9));
  }

  @Test
  void noConfiguredDelayMeansNoDelayWhateverTheJitter() {
    var policy = RetryPolicy.of(1).withJitter(1.0);
    var retry = (RetryDecision.Retry) policy.decide(0, true, T0);
    assertEquals(StateType.RUNNING, retry.state().type());
  }

  private static Instant scheduledFor(RetryPolicy policy, int retriesSpent) {
    return ((RetryDecision.Retry) policy.decide(retriesSpent, true, T0)).state().scheduledTime();
  }

  @Test
  void refusalsCarryTheReasonSoACallerCanTellThemApart() {
    assertEquals("Retries are exhausted", ((RetryDecision.Exhausted) RetryPolicy.of(0).decide(0, true, T0)).reason());
    assertEquals(
        "The retry condition answered no",
        ((RetryDecision.NotRetriable) RetryPolicy.of(1).decide(0, false, T0)).reason());
  }

  @Test
  void aDurationIsWhatTheStateCarriesNotAFloat() {
    var retry = (RetryDecision.Retry) RetryPolicy.withDelays(1, List.of(1.5)).decide(0, true, T0);
    assertEquals(T0.plus(Duration.ofMillis(1500)), retry.state().scheduledTime());
  }
}
