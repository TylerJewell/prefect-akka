package io.akka.prefect.domain;

/**
 * What a cache policy can read about the run it is computing a key for. Any of the three may be
 * absent: a key computed outside a run has no identifiers, and a task whose declared source text
 * was not supplied cannot contribute one (SPEC-001 §4.2).
 */
public record TaskRunContext(String taskRunId, String flowRunId, String sourceText) {}
