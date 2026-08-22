package io.akka.prefect.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * A cache key is the MD5 digest of a JSON rendering, and it has to be the same digest Prefect
 * produces or the two systems would miss each other's cache entries. The rendering is Python's
 * {@code json.dumps(sort_keys=True)}: keys sorted, {@code ", "} between elements, {@code ": "}
 * after a key, non-ASCII escaped, and the arguments wrapped as {@code [[...], {}]} — the
 * positional-and-keyword tuple Prefect hashes.
 */
public final class CacheKey {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private CacheKey() {}

  public static String digest(JsonValue value) {
    return digest(List.of(value));
  }

  public static String digest(List<JsonValue> values) {
    return md5(hashedText(values));
  }

  /** The keys of a compound policy's members, hashed in member order rather than sorted. */
  public static String digestOfKeys(List<String> keys) {
    var values = new ArrayList<JsonValue>(keys.size());
    for (var key : keys) {
      values.add(JsonValue.str(key));
    }
    return digest(values);
  }

  /** The exact text whose bytes are digested — the thing a disagreement with Prefect shows up in. */
  public static String hashedText(List<JsonValue> values) {
    var out = new StringBuilder("[[");
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        out.append(", ");
      }
      render(values.get(i), out);
    }
    return out.append("], {}]").toString();
  }

  private static void render(JsonValue value, StringBuilder out) {
    switch (value) {
      case JsonValue.JNull ignored -> out.append("null");
      case JsonValue.JBool b -> out.append(b.value() ? "true" : "false");
      case JsonValue.JInt i -> out.append(i.value());
      case JsonValue.JDbl d -> out.append(renderDouble(d.value()));
      case JsonValue.JStr s -> renderString(s.value(), out);
      case JsonValue.JArr a -> {
        out.append('[');
        for (int i = 0; i < a.values().size(); i++) {
          if (i > 0) {
            out.append(", ");
          }
          render(a.values().get(i), out);
        }
        out.append(']');
      }
      case JsonValue.JObj o -> {
        out.append('{');
        var keys = new ArrayList<>(o.fields().keySet());
        keys.sort(String::compareTo);
        for (int i = 0; i < keys.size(); i++) {
          if (i > 0) {
            out.append(", ");
          }
          renderString(keys.get(i), out);
          out.append(": ");
          render(o.fields().get(keys.get(i)), out);
        }
        out.append('}');
      }
    }
  }

  private static String renderDouble(double d) {
    if (d == Math.rint(d) && !Double.isInfinite(d)) {
      return (long) d + ".0";
    }
    return Double.toString(d);
  }

  private static void renderString(String s, StringBuilder out) {
    out.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        default -> {
          if (c < 0x20 || c > 0x7e) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
  }

  private static String md5(String text) {
    try {
      var digest = MessageDigest.getInstance("MD5");
      return HexFormat.of().formatHex(digest.digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 is required to reproduce Prefect's cache keys", e);
    }
  }
}
