package io.akka.prefect.domain;

import static io.akka.prefect.domain.JsonValue.arr;
import static io.akka.prefect.domain.JsonValue.bool;
import static io.akka.prefect.domain.JsonValue.num;
import static io.akka.prefect.domain.JsonValue.obj;
import static io.akka.prefect.domain.JsonValue.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The digests on the right were produced by Prefect itself, by
 * {@code prefect-port/probes/p1_hash_shape.py}. They are the whole point of this class:
 * a key that does not match them byte for byte is a key that would miss the original's
 * cache.
 */
class CacheKeyTest {

  @Test
  void digestMatchesPrefectForKnownValues() {
    assertEquals("599d6780d743a1b50a942acff196a0f1", CacheKey.digest(obj(Map.of("x", num(1)))));
    assertEquals(
        "1eda3722b3639ef1264c554c32f7dd4e",
        CacheKey.digest(obj(Map.of("b", num(2), "a", num(1)))));
    assertEquals(
        "d302df9842e68862360c78f53252c097",
        CacheKey.digest(
            obj(Map.of("s", str("hello"), "n", JsonValue.NULL, "f", num(1.5), "t", bool(true)))));
    assertEquals(
        "2d9b42ecd878645bdb22e792e48a56e6",
        CacheKey.digest(obj(Map.of("l", arr(List.of(num(1), num(2), num(3)))))));
    assertEquals("05da934bc95640755e0d7882f6f171d0", CacheKey.digest(obj(Map.of())));
  }

  @Test
  void keysAreSortedBeforeHashingSoInsertionOrderCannotShowThrough() {
    assertEquals(
        CacheKey.digest(obj(Map.of("x", num(1), "y", num(2)))),
        CacheKey.digest(obj(Map.of("y", num(2), "x", num(1)))));
    assertEquals(
        "935c832359f6b6b1222ab2d2f271d6be", CacheKey.digest(obj(Map.of("x", num(1), "y", num(2)))));
  }

  @Test
  void compoundDigestIsOrderSensitive() {
    assertEquals("ba45fa969bb2acdcf6c5b1b8f07410eb", CacheKey.digestOfKeys(List.of("k1", "k2")));
    assertEquals("7a5251da2d8cf8356b48db3d58b65b9a", CacheKey.digestOfKeys(List.of("k2", "k1")));
  }

  @Test
  void renderingUsesPythonsSeparatorsAndNoOtherWhitespace() {
    assertEquals(
        "[[{\"a\": 1, \"b\": [1, 2]}], {}]",
        CacheKey.hashedText(List.of(obj(Map.of("b", arr(List.of(num(1), num(2))), "a", num(1))))));
  }

  @Test
  void nonJsonValueFailsTheComputation() {
    assertThrows(IllegalArgumentException.class, () -> JsonValue.of(new Object()));
  }
}
