package io.akka.prefect.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class BackoffTest {

  @Test
  void eachStepDoublesTheFactor() {
    assertEquals(List.of(1.0, 2.0, 4.0), Backoff.exponential(1, 3));
    assertEquals(List.of(2.0, 4.0, 8.0), Backoff.exponential(2, 3));
    assertEquals(List.of(10.0, 20.0, 40.0), Backoff.exponential(10, 3));
  }

  @Test
  void aCountOfZeroGivesNothingToWaitFor() {
    assertEquals(List.of(), Backoff.exponential(5, 0));
  }

  @Test
  void theCountIsCappedAtFifty() {
    assertEquals(50, Backoff.exponential(1, 60).size());
    assertEquals(50, Backoff.exponential(1, 50).size());
    assertEquals(49, Backoff.exponential(1, 49).size());
  }
}
