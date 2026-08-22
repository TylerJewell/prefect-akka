package io.akka.prefect.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A value a cache key may be computed from. Prefect hashes the JSON rendering of a task's
 * arguments and falls back to a pickle when JSON cannot express them; this port accepts only
 * what JSON can express, and {@link #of} refuses the rest rather than inventing a second
 * encoding (SPEC-001 §4.1).
 */
public sealed interface JsonValue {

  record JStr(String value) implements JsonValue {}

  record JInt(long value) implements JsonValue {}

  record JDbl(double value) implements JsonValue {}

  record JBool(boolean value) implements JsonValue {}

  record JNull() implements JsonValue {}

  record JArr(List<JsonValue> values) implements JsonValue {}

  record JObj(Map<String, JsonValue> fields) implements JsonValue {}

  JNull NULL = new JNull();

  static JsonValue str(String v) {
    return new JStr(v);
  }

  static JsonValue num(long v) {
    return new JInt(v);
  }

  static JsonValue num(double v) {
    return new JDbl(v);
  }

  static JsonValue bool(boolean v) {
    return new JBool(v);
  }

  static JArr arr(List<JsonValue> v) {
    return new JArr(List.copyOf(v));
  }

  static JObj obj(Map<String, JsonValue> v) {
    return new JObj(Map.copyOf(v));
  }

  /**
   * Lifts an ordinary Java value into a JSON value, refusing anything JSON has no form for.
   * The refusal is the behaviour Prefect has for an argument it can neither serialise nor
   * pickle: the run fails rather than quietly skipping the cache.
   */
  static JsonValue of(Object value) {
    if (value == null) {
      return NULL;
    }
    if (value instanceof JsonValue already) {
      return already;
    }
    if (value instanceof String s) {
      return new JStr(s);
    }
    if (value instanceof Boolean b) {
      return new JBool(b);
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return new JInt(((Number) value).longValue());
    }
    if (value instanceof Float || value instanceof Double) {
      return new JDbl(((Number) value).doubleValue());
    }
    if (value instanceof List<?> list) {
      var out = new ArrayList<JsonValue>(list.size());
      for (var item : list) {
        out.add(of(item));
      }
      return new JArr(List.copyOf(out));
    }
    if (value instanceof Map<?, ?> map) {
      var out = new LinkedHashMap<String, JsonValue>();
      for (var entry : map.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw new IllegalArgumentException(
              "A cache key input object needs string keys; got " + entry.getKey().getClass().getName());
        }
        out.put(key, of(entry.getValue()));
      }
      return new JObj(Map.copyOf(out));
    }
    throw new IllegalArgumentException(
        "A cache key input must be a JSON value; "
            + value.getClass().getName()
            + " is not one. Exclude the argument, or use a key function.");
  }
}
