package io.akka.prefect.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BiFunction;

/**
 * How a task run's cache key is computed. Policies compose with {@link #plus} and narrow with
 * {@link #minus}; a policy that has nothing to say returns {@code null}, and a compound made
 * entirely of those says nothing either.
 */
public sealed interface CachePolicy {

  /** Storage, isolation and locking travel with a policy and are compared when two are added. */
  record Settings(String keyStorage, String isolationLevel, String lockManager) {
    static final Settings NONE = new Settings(null, null, null);
  }

  Settings settings();

  CachePolicy withSettings(Settings settings);

  String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters);

  default String keyStorage() {
    return settings().keyStorage();
  }

  default String isolationLevel() {
    return settings().isolationLevel();
  }

  default String lockManager() {
    return settings().lockManager();
  }

  default CachePolicy configure(String keyStorage, String isolationLevel, String lockManager) {
    return withSettings(
        new Settings(
            keyStorage != null ? keyStorage : keyStorage(),
            isolationLevel != null ? isolationLevel : isolationLevel(),
            lockManager != null ? lockManager : lockManager()));
  }

  /** Narrowing by an argument name. Only policies that read the arguments have anything to narrow. */
  default CachePolicy minus(String name) {
    return this;
  }

  default CachePolicy plus(CachePolicy other) {
    if (other instanceof NoCache) {
      return this;
    }
    if (this instanceof NoCache) {
      return other;
    }
    var merged = merge(settings(), other.settings());
    var members = new ArrayList<CachePolicy>();
    if (this instanceof Compound c) {
      members.addAll(c.policies());
    } else {
      members.add(this);
    }
    if (other instanceof Compound c) {
      members.addAll(c.policies());
    } else {
      members.add(other);
    }
    return new Compound(members, merged);
  }

  private static Settings merge(Settings a, Settings b) {
    conflict("storage locations", a.keyStorage(), b.keyStorage());
    conflict("isolation levels", a.isolationLevel(), b.isolationLevel());
    conflict("lock implementations", a.lockManager(), b.lockManager());
    return new Settings(
        a.keyStorage() != null ? a.keyStorage() : b.keyStorage(),
        a.isolationLevel() != null ? a.isolationLevel() : b.isolationLevel(),
        a.lockManager() != null ? a.lockManager() : b.lockManager());
  }

  private static void conflict(String what, String a, String b) {
    if (a != null && b != null && !a.equals(b)) {
      throw new IllegalArgumentException("Cannot add CachePolicies with different " + what + ".");
    }
  }

  record Inputs(List<String> exclude, Settings settings) implements CachePolicy {
    public Inputs {
      exclude = List.copyOf(exclude);
    }

    private java.util.Set<String> excluded() {
      return exclude.isEmpty() ? java.util.Set.of() : java.util.Set.copyOf(exclude);
    }

    @Override
    public CachePolicy withSettings(Settings s) {
      return new Inputs(exclude, s);
    }

    @Override
    public CachePolicy minus(String name) {
      var wider = new ArrayList<>(exclude);
      wider.add(name);
      return new Inputs(wider, settings);
    }

    /**
     * Absent arguments and fully excluded arguments are different answers: given nothing there
     * is no key, whereas excluding everything that was given leaves the digest of an empty
     * object, which is a key like any other.
     */
    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      if (inputs == null || inputs.fields().isEmpty()) {
        return null;
      }
      var excluded = excluded();
      var kept = new LinkedHashMap<String, JsonValue>();
      inputs.fields().forEach((name, value) -> {
        if (!excluded.contains(name)) {
          kept.put(name, value);
        }
      });
      return CacheKey.digest(JsonValue.obj(kept));
    }
  }

  record TaskSource(Settings settings) implements CachePolicy {
    @Override
    public CachePolicy withSettings(Settings s) {
      return new TaskSource(s);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      if (context == null || context.sourceText() == null) {
        return null;
      }
      return CacheKey.digest(JsonValue.str(context.sourceText()));
    }
  }

  record FlowParameters(Settings settings) implements CachePolicy {
    @Override
    public CachePolicy withSettings(Settings s) {
      return new FlowParameters(s);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      if (flowParameters == null || flowParameters.fields().isEmpty()) {
        return null;
      }
      return CacheKey.digest(flowParameters);
    }
  }

  record RunId(Settings settings) implements CachePolicy {
    @Override
    public CachePolicy withSettings(Settings s) {
      return new RunId(s);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      if (context == null) {
        return null;
      }
      return context.flowRunId() != null ? context.flowRunId() : context.taskRunId();
    }
  }

  record NoCache(Settings settings) implements CachePolicy {
    @Override
    public CachePolicy withSettings(Settings s) {
      return new NoCache(s);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      return null;
    }
  }

  record KeyFunction(BiFunction<TaskRunContext, JsonValue.JObj, String> fn, Settings settings)
      implements CachePolicy {
    @Override
    public CachePolicy withSettings(Settings s) {
      return new KeyFunction(fn, s);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      return fn.apply(context, inputs);
    }
  }

  /**
   * Members are canonicalised on construction: nested compounds are spliced in place, and every
   * {@code Inputs} is replaced by one carrying the sorted union of their exclusions, appended last.
   * So a compound's declared order survives for every other kind of member — and it matters,
   * because the members' keys are hashed in that order.
   */
  record Compound(List<CachePolicy> policies, Settings settings) implements CachePolicy {
    public Compound {
      var flattened = new ArrayList<CachePolicy>();
      for (var member : policies) {
        if (member instanceof Compound nested) {
          flattened.addAll(nested.policies());
        } else {
          flattened.add(member);
        }
      }
      var exclusions = new TreeSet<String>();
      boolean anyInputs = false;
      var kept = new ArrayList<CachePolicy>();
      for (var member : flattened) {
        if (member instanceof Inputs in) {
          anyInputs = true;
          exclusions.addAll(in.exclude());
        } else {
          kept.add(member);
        }
      }
      if (anyInputs) {
        kept.add(new Inputs(List.copyOf(exclusions), Settings.NONE));
      }
      policies = List.copyOf(kept);
    }

    @Override
    public CachePolicy withSettings(Settings s) {
      return new Compound(policies, s);
    }

    @Override
    public CachePolicy minus(String name) {
      if (policies.stream().noneMatch(p -> p instanceof Inputs)) {
        return this;
      }
      var wider = new ArrayList<>(policies);
      wider.add(new Inputs(List.of(name), Settings.NONE));
      return new Compound(wider, settings);
    }

    @Override
    public String computeKey(TaskRunContext context, JsonValue.JObj inputs, JsonValue.JObj flowParameters) {
      var keys = new ArrayList<String>();
      for (var member : policies) {
        var key = member.computeKey(context, inputs, flowParameters);
        if (key != null) {
          keys.add(key);
        }
      }
      return keys.isEmpty() ? null : CacheKey.digestOfKeys(keys);
    }
  }

  CachePolicy INPUTS = new Inputs(List.of(), Settings.NONE);
  CachePolicy TASK_SOURCE = new TaskSource(Settings.NONE);
  CachePolicy FLOW_PARAMETERS = new FlowParameters(Settings.NONE);
  CachePolicy RUN_ID = new RunId(Settings.NONE);
  CachePolicy NO_CACHE = new NoCache(Settings.NONE);
  CachePolicy DEFAULT = INPUTS.plus(TASK_SOURCE).plus(RUN_ID);

  static CachePolicy inputs(String... exclude) {
    return new Inputs(List.of(exclude), Settings.NONE);
  }

  static CachePolicy keyFunction(BiFunction<TaskRunContext, JsonValue.JObj, String> fn) {
    return new KeyFunction(Objects.requireNonNull(fn), Settings.NONE);
  }

  static CachePolicy compound(CachePolicy... members) {
    return new Compound(List.of(members), Settings.NONE);
  }
}
