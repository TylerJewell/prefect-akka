# prefect-akka

Works out whether a task run can reuse an earlier result, decides whether a failed attempt
is tried again and after how long, and keeps a run's own record of when it started, how
long it ran and how many times.

A port of [PrefectHQ/prefect](https://github.com/PrefectHQ/prefect) onto **Akka**, built
with **Akka Specify**.

---

## Where it came from

PrefectHQ/prefect is a Python system for running data pipelines: you mark functions as
tasks, it runs them, remembers what happened, retries the ones that fail and skips the ones
whose answer it already has. It was ported to derive a specification format precise enough
to regenerate a system on a different stack — the port is the vehicle, the specification is
the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `prefect-port/`.

---

## PrefectHQ/prefect → this port

📉 696 Python lines → **559 Java lines**<br>
📁 7 files → **10 files**<br>
⚡ 29,297 → **614** nanoseconds to work out one reuse key<br>
🎯 38 of 38 → **38 of 38** comparisons giving the same answer<br>
🧪 not measured → **22 of 22** deliberate breakages caught by a check

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/prefect-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.5 hours** from the first command to the published repository, **1.5** of them active<br>
💬 **268** exchanges with the model<br>
✍️ **288,254** tokens written by the model, **55,549,727** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **80** tests

```bash
python toolkit/tokens.py --port prefect    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

From the specification:

- **A reuse key is the same 32 characters here as it is in Prefect, or it is worthless.**
  Two systems that work out the key differently would never find each other's saved
  results, so the key is built from the exact same text, down to the spaces between the
  parts and the order the parts are written in.
- **Leaving out every argument is not the same as having none.** A task called with no
  arguments has no key at all; a task called with arguments that are all deliberately
  ignored still gets one.
- **Ways of building a key can be added together, and the order they are added in
  matters** — except for the one that reads the task's arguments, which always ends up
  last however it was written.
- **A budget of two retries means the task runs three times.** After each failure the run
  either waits a set number of seconds and tries again, or tries again straight away, and
  the run's own record says which happened.
- **When a list of waiting times runs out, the last one repeats.** Waits of one and two
  seconds against a budget of five gives one, two, two, two, two.
- **A run's own record is worked out from the states it passed through, never set
  directly.** The moment it started, the moment it ended, how long it spent actually
  running, and how many times it started, all follow from the sequence, so the record and
  the history can never say different things.
- **Time spent waiting for a retry is not time spent running.** A run that waits thirty
  seconds between two three-second attempts has run for six seconds.
- **Coming back from a finished state unfinishes it.** The moment it ended is forgotten,
  the moment it started is kept, and the time it had already run is kept too.

---

## Design decisions

**Event sourcing.** Every state a run passes through is written down as it happens, and
the run's summary is worked out by reading that list back from the start. The summary can
never drift away from what actually happened, because it is not stored — it is recalculated
every time.

**Stamping the time at the door.** The moment a state happened is fixed when it arrives and
never taken from whoever sent it. Reading the list back gives the same answer today as it
will next year, which would not be true if the clock were read while reading.

**Keys as text, not as objects.** A reuse key is built by writing the values out as text in
one exact agreed form and then squashing that text down to 32 characters. Two programs
written in different languages can agree on text; they cannot agree on how each stores a
number in memory.

**Only what a note can hold.** A value that cannot be written out as ordinary text is
refused outright rather than quietly skipped. A skipped value would mean two different
calls sharing one key and one of them getting the other's answer back.

**A cap on how much history is kept.** The most recent thousand states are kept and the
rest are forgotten, though the count of all of them is not. A record that only ever grew
would eventually be too big to copy between machines, and the summary needs no state older
than the one before it.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/prefect-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9048.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9048**.

### Try it

```bash
# work out a reuse key
curl -s localhost:9048/cache-keys/compute -H 'content-type: application/json' \
  -d '{"policies":[{"kind":"inputs"}],"inputs":{"x":42}}'

# give a run a budget of two retries, then fail it twice
curl -s localhost:9048/task-runs/run-1/retry-budget -H 'content-type: application/json' \
  -d '{"budget":2,"delays":[],"jitterFactor":0}'
curl -s localhost:9048/task-runs/run-1/states -H 'content-type: application/json' \
  -d '{"type":"RUNNING","name":"Running"}'
curl -s localhost:9048/task-runs/run-1/failures -H 'content-type: application/json' \
  -d '{"message":"boom","retriable":true}'
curl -s localhost:9048/task-runs/run-1
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | The service reads no environment variables. The port it listens on is set in `src/main/resources/application.conf`. |

This port calls no model provider, so it needs no key for one.

---

## Where it differs from PrefectHQ/prefect

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Values that cannot be written out as ordinary text.** Prefect first tries to write the
  task's arguments out as text and, when that fails, falls back to a second way of turning
  them into bytes that only Python understands, so two Python objects with no text form
  still get a key. This port refuses them and fails the call, because that second way
  depends on details of the Python language that do not exist here, and a port claiming to
  reproduce it would be claiming something no check could confirm.
- **Where the task's own text comes from.** Prefect reads the source text of the task
  function out of the running program, with three fallbacks for when it cannot find it.
  This port is handed the text instead, because "find the Python text of this function" is
  a question that has no meaning outside Python; the rule being copied — that the key is
  built from the task's declared text — is unchanged.
- **Spreading retry waits out at random.** Prefect can spread a waiting time over a random
  range so that many runs failing at once do not all come back at the same moment. On the
  path a task run takes on one machine this has no effect at all, which was checked by
  running it six times: every wait was identical. It only takes effect inside Prefect's
  server. This port has the same spreading arithmetic but a fixed default, and takes the
  random draw as something the caller passes in, because a random wait cannot be compared
  against the original one input at a time.
- **The same state written down twice in a row.** Prefect's own code never does this, so
  there is no behaviour to copy. This port applies its rules again, which means a repeated
  running state counts as two starts. The alternative — noticing the repeat and ignoring it
  — would also be an invention, and this one is the simpler of the two.
- **How much history is kept.** Prefect stores every state a run ever entered, in a table
  of its own. This port keeps the most recent thousand and a count of all of them, so that
  a very long-lived run's record stays small enough to copy between machines. Nothing the
  summary reports depends on a state older than the one before it.
- **Reading a run's states as they happen.** Prefect's own interface follows a run live.
  This port answers a question when asked and does not push changes out. **Not checked**
  against the original — no comparison was run, because this port has no screen to compare.
- **What happens when two callers write to the same run at once.** **Not checked** on
  either side.
- **Sorting of key names that need more than the common range of characters.** Both sides
  sort the names inside a key before turning them into text, but they compare characters by
  slightly different rules, which can only disagree for names built from the rarest
  characters — emoji and some historic scripts. **Not checked**; no such name was tested.

---

## Licence

PrefectHQ/prefect is under the Apache License 2.0, © 2019- Prefect Technologies, Inc. This
port reimplements the behaviour without copied source; see `ACKNOWLEDGEMENTS.md`.
