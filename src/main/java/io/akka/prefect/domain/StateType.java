package io.akka.prefect.domain;

/** The types a recorded state can have. Four of them end a run. */
public enum StateType {
  SCHEDULED,
  PENDING,
  RUNNING,
  COMPLETED,
  FAILED,
  CANCELLED,
  CRASHED,
  PAUSED,
  CANCELLING;

  public boolean isFinal() {
    return this == COMPLETED || this == FAILED || this == CANCELLED || this == CRASHED;
  }
}
