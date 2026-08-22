package io.akka.prefect.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class CachePolicyAlgebraTest {

  @Test
  void addingNoCacheIsNothingOnEitherSide() {
    assertSame(CachePolicy.INPUTS, CachePolicy.NO_CACHE.plus(CachePolicy.INPUTS));
    assertSame(CachePolicy.INPUTS, CachePolicy.INPUTS.plus(CachePolicy.NO_CACHE));
  }

  @Test
  void addingTwoLeavesMakesACompound() {
    var sum = CachePolicy.TASK_SOURCE.plus(CachePolicy.RUN_ID);
    assertInstanceOf(CachePolicy.Compound.class, sum);
    assertEquals(2, ((CachePolicy.Compound) sum).policies().size());
  }

  @Test
  void addingConflictingKeyStorageFails() {
    var a = CachePolicy.INPUTS.configure("bucket-a", null, null);
    var b = CachePolicy.TASK_SOURCE.configure("bucket-b", null, null);
    var thrown = assertThrows(IllegalArgumentException.class, () -> a.plus(b));
    assertTrue(thrown.getMessage().contains("storage"));
  }

  @Test
  void addingConflictingIsolationLevelFails() {
    var a = CachePolicy.INPUTS.configure(null, "SERIALIZABLE", null);
    var b = CachePolicy.TASK_SOURCE.configure(null, "READ_COMMITTED", null);
    var thrown = assertThrows(IllegalArgumentException.class, () -> a.plus(b));
    assertTrue(thrown.getMessage().contains("isolation"));
  }

  @Test
  void addingConflictingLockManagerFails() {
    var a = CachePolicy.INPUTS.configure(null, null, "lock-a");
    var b = CachePolicy.TASK_SOURCE.configure(null, null, "lock-b");
    var thrown = assertThrows(IllegalArgumentException.class, () -> a.plus(b));
    assertTrue(thrown.getMessage().contains("lock"));
  }

  @Test
  void addingCompatibleSettingsCarriesThemOntoTheCompound() {
    var sum = CachePolicy.INPUTS.configure("bucket", null, null).plus(CachePolicy.TASK_SOURCE.configure(null, "SERIALIZABLE", null));
    assertEquals("bucket", sum.keyStorage());
    assertEquals("SERIALIZABLE", sum.isolationLevel());
  }

  @Test
  void subtractingFromInputsExtendsTheExclusionList() {
    var minus = (CachePolicy.Inputs) CachePolicy.INPUTS.minus("x");
    assertEquals(List.of("x"), minus.exclude());
  }

  @Test
  void subtractingFromAnyOtherLeafChangesNothing() {
    assertSame(CachePolicy.TASK_SOURCE, CachePolicy.TASK_SOURCE.minus("x"));
    assertSame(CachePolicy.RUN_ID, CachePolicy.RUN_ID.minus("x"));
    assertSame(CachePolicy.FLOW_PARAMETERS, CachePolicy.FLOW_PARAMETERS.minus("x"));
    assertSame(CachePolicy.NO_CACHE, CachePolicy.NO_CACHE.minus("x"));
  }

  @Test
  void subtractingFromACompoundThatHasInputsExcludesTheName() {
    var minus = (CachePolicy.Compound) CachePolicy.DEFAULT.minus("x");
    assertEquals(
        List.of("TaskSource", "RunId", "Inputs"),
        minus.policies().stream().map(p -> p.getClass().getSimpleName()).toList());
    assertEquals(List.of("x"), ((CachePolicy.Inputs) minus.policies().get(2)).exclude());
  }

  @Test
  void subtractingFromACompoundWithoutInputsChangesNothing() {
    var compound = CachePolicy.compound(CachePolicy.TASK_SOURCE, CachePolicy.RUN_ID);
    assertSame(compound, compound.minus("x"));
  }

  @Test
  void addingToACompoundAppends() {
    var sum = (CachePolicy.Compound) CachePolicy.compound(CachePolicy.TASK_SOURCE).plus(CachePolicy.RUN_ID);
    assertEquals(
        List.of("TaskSource", "RunId"),
        sum.policies().stream().map(p -> p.getClass().getSimpleName()).toList());
  }
}
