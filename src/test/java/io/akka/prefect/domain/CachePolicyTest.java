package io.akka.prefect.domain;

import static io.akka.prefect.domain.JsonValue.num;
import static io.akka.prefect.domain.JsonValue.obj;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CachePolicyTest {

  private static final JsonValue.JObj INPUTS_X1 = obj(Map.of("x", num(1)));
  private static final JsonValue.JObj PARAMS_A1 = obj(Map.of("a", num(1)));
  private static final TaskRunContext CTX =
      new TaskRunContext("00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000001", "def t(): ...");

  private static List<String> kinds(CachePolicy p) {
    return ((CachePolicy.Compound) p).policies().stream().map(m -> m.getClass().getSimpleName()).toList();
  }

  @Test
  void defaultPolicyPutsInputsLastWhateverTheOrderItWasWrittenIn() {
    var declared = CachePolicy.INPUTS.plus(CachePolicy.TASK_SOURCE).plus(CachePolicy.RUN_ID);
    assertEquals(List.of("TaskSource", "RunId", "Inputs"), kinds(declared));
    assertEquals(List.of("TaskSource", "RunId", "Inputs"), kinds(CachePolicy.DEFAULT));
  }

  @Test
  void compoundFlattensNestedCompounds() {
    var nested =
        CachePolicy.compound(
            CachePolicy.compound(CachePolicy.TASK_SOURCE, CachePolicy.RUN_ID),
            CachePolicy.FLOW_PARAMETERS);
    assertEquals(List.of("TaskSource", "RunId", "FlowParameters"), kinds(nested));
  }

  @Test
  void compoundMovesInputsLastAndMergesExclusions() {
    var merged =
        CachePolicy.compound(
            CachePolicy.inputs("b"), CachePolicy.TASK_SOURCE, CachePolicy.inputs("a", "b"));
    assertEquals(List.of("TaskSource", "Inputs"), kinds(merged));
    var inputs = (CachePolicy.Inputs) ((CachePolicy.Compound) merged).policies().get(1);
    assertEquals(List.of("a", "b"), inputs.exclude());
  }

  @Test
  void compoundKeyIsOrderSensitiveAmongPoliciesThatAreNotInputs() {
    var forward = CachePolicy.compound(CachePolicy.FLOW_PARAMETERS, CachePolicy.RUN_ID);
    var reversed = CachePolicy.compound(CachePolicy.RUN_ID, CachePolicy.FLOW_PARAMETERS);
    assertTrue(
        !forward.computeKey(CTX, INPUTS_X1, PARAMS_A1).equals(reversed.computeKey(CTX, INPUTS_X1, PARAMS_A1)));
  }

  @Test
  void compoundKeyIsTheDigestOfItsMembersKeys() {
    var compound = CachePolicy.compound(CachePolicy.FLOW_PARAMETERS, CachePolicy.INPUTS);
    assertEquals("286dd9e9d7928846eacdcc42842d4f0a", compound.computeKey(CTX, INPUTS_X1, PARAMS_A1));
  }

  @Test
  void compoundDropsMembersWithNoKey() {
    var compound = CachePolicy.compound(CachePolicy.NO_CACHE, CachePolicy.INPUTS);
    assertEquals(
        CacheKey.digestOfKeys(List.of(CachePolicy.INPUTS.computeKey(CTX, INPUTS_X1, PARAMS_A1))),
        compound.computeKey(CTX, INPUTS_X1, PARAMS_A1));
    assertNull(
        CachePolicy.compound(CachePolicy.NO_CACHE, CachePolicy.NO_CACHE)
            .computeKey(CTX, INPUTS_X1, PARAMS_A1));
  }

  @Test
  void compoundOfOneIsNotTheMemberKey() {
    var compound = CachePolicy.compound(CachePolicy.INPUTS);
    assertEquals("480ad33bff2e7ab6d1ed7f2dacb0f84d", compound.computeKey(CTX, INPUTS_X1, PARAMS_A1));
    assertEquals("599d6780d743a1b50a942acff196a0f1", CachePolicy.INPUTS.computeKey(CTX, INPUTS_X1, PARAMS_A1));
  }

  @Test
  void leafPoliciesProduceNoKeyWhenTheirInputIsAbsent() {
    var empty = obj(Map.of());
    assertNull(CachePolicy.INPUTS.computeKey(CTX, empty, PARAMS_A1));
    assertNull(CachePolicy.INPUTS.computeKey(CTX, null, PARAMS_A1));
    assertNull(CachePolicy.FLOW_PARAMETERS.computeKey(CTX, INPUTS_X1, empty));
    assertNull(CachePolicy.FLOW_PARAMETERS.computeKey(CTX, INPUTS_X1, null));
    assertNull(CachePolicy.RUN_ID.computeKey(null, INPUTS_X1, PARAMS_A1));
    assertNull(CachePolicy.TASK_SOURCE.computeKey(null, INPUTS_X1, PARAMS_A1));
    assertNull(CachePolicy.NO_CACHE.computeKey(CTX, INPUTS_X1, PARAMS_A1));
  }

  @Test
  void inputsExclusionMatchesOmission() {
    assertEquals(
        "692453033631ac5e60162a07eb1797ba",
        CachePolicy.INPUTS.computeKey(CTX, obj(Map.of("x", num(42))), null));
    assertEquals(
        "692453033631ac5e60162a07eb1797ba",
        CachePolicy.inputs("y").computeKey(CTX, obj(Map.of("x", num(42), "y", num(1))), null));
  }

  @Test
  void excludingEveryArgumentIsNotTheSameAsHavingNone() {
    assertEquals(
        "05da934bc95640755e0d7882f6f171d0",
        CachePolicy.inputs("x").computeKey(CTX, obj(Map.of("x", num(42))), null));
    assertNull(CachePolicy.INPUTS.computeKey(CTX, obj(Map.of()), null));
  }

  @Test
  void flowParametersKeyMatchesPrefect() {
    assertEquals("2576c7382d4326599e7292b78724a71e", CachePolicy.FLOW_PARAMETERS.computeKey(CTX, null, PARAMS_A1));
  }

  @Test
  void runIdPrefersFlowRunId() {
    assertEquals(
        "00000000-0000-0000-0000-000000000001", CachePolicy.RUN_ID.computeKey(CTX, null, null));
    var noFlow = new TaskRunContext("00000000-0000-0000-0000-000000000002", null, "def t(): ...");
    assertEquals(
        "00000000-0000-0000-0000-000000000002", CachePolicy.RUN_ID.computeKey(noFlow, null, null));
  }

  @Test
  void taskSourceHashesTheDeclaredSourceText() {
    assertEquals(
        CacheKey.digest(JsonValue.str("def t(): ...")),
        CachePolicy.TASK_SOURCE.computeKey(CTX, null, null));
    assertNull(CachePolicy.TASK_SOURCE.computeKey(new TaskRunContext("a", null, null), null, null));
  }

  @Test
  void keyFunctionResultIsUsedVerbatim() {
    assertEquals(
        "static-key",
        CachePolicy.keyFunction((ctx, in) -> "static-key").computeKey(CTX, INPUTS_X1, PARAMS_A1));
    assertNull(CachePolicy.keyFunction((ctx, in) -> null).computeKey(CTX, INPUTS_X1, PARAMS_A1));
  }

  @Test
  void unserialisableArgumentFailsRatherThanSkippingTheCache() {
    assertThrows(IllegalArgumentException.class, () -> JsonValue.of(Thread.currentThread()));
  }

  @Test
  void aCompoundIsStillACompound() {
    assertInstanceOf(CachePolicy.Compound.class, CachePolicy.DEFAULT);
  }
}
