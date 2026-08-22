# Acknowledgements

This project is a port of **[PrefectHQ/prefect](https://github.com/PrefectHQ/prefect)**.

## Licence and copyright

- PrefectHQ/prefect is licensed under the **Apache License 2.0**. Copyright 2019- Prefect
  Technologies, Inc. (`prefect-src/LICENSE:189`).
- **Nothing was copied verbatim.** Every Java file under `prefect-akka/src` was written
  fresh against behaviour read out of, and run against, the Python source; no source text,
  comments, or test fixtures were transcribed. Where a comment or a document cites a source
  file and line range, that is citation, not copying.
- **Digests are not copied text.** The 32-character MD5 values asserted in `CacheKeyTest`
  and `CachePolicyTest` were produced by running Prefect and are the observed output of the
  behaviour being reproduced, which is the whole point of a parity test.
- **Behaviour is derived throughout**, plainly: the cache-key policies, the retry decision
  and delay selection, and the nine bookkeeping rules a run's record follows are a direct
  port of the decision procedures in `src/prefect/cache_policies.py`,
  `src/prefect/utilities/hashing.py`, `src/prefect/task_engine.py`,
  `src/prefect/server/orchestration/global_policy.py` and
  `src/prefect/server/orchestration/core_policy.py`. This is the nature of a port and is
  not something to obscure.
- Because no Apache-2.0 text was copied into this repository, nothing here is bound by
  PrefectHQ/prefect's licence terms — the "copied material carries its licence with it"
  rule does not trigger, since nothing was copied. `LICENSE-prefect` carries the original
  licence text for reference and attribution only.

## Also used

- [Akka](https://akka.io) — the SDK and runtime this port is built on.
