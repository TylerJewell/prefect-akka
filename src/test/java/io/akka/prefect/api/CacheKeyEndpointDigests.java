package io.akka.prefect.api;

/** Digests produced by Prefect itself, so a runtime-backed check compares against the original. */
final class CacheKeyEndpointDigests {

  /** A compound of one inputs policy over {@code {"x": 42}}. */
  static final String INPUTS_X42_COMPOUND = "5f19c162781d0ef37232e2d71f1bd41f";

  private CacheKeyEndpointDigests() {}
}
