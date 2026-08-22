package io.akka.prefect.domain;

import java.time.Instant;

/**
 * One state as it is recorded. The name is free text alongside the type — {@code Retrying} is a
 * running state and {@code AwaitingRetry} a scheduled one — and the scheduled time is set only
 * where the type is scheduled.
 */
public record RunState(StateType type, String name, Instant timestamp, Instant scheduledTime) {

  public static RunState of(StateType type, String name, Instant timestamp) {
    return new RunState(type, name, timestamp, null);
  }
}
