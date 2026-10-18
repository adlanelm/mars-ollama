---
description: Plan a change as small steps without touching anything; wait for OK before implementing
argument-hint: "<task>"
---
[plan-only] Plan: $@

Look at the code involved first (read it, or ask scouts what exists), and reuse what is there.
Reply with numbered steps, one line each:
  N. <what> | files: <1-2 full paths; mark new ones (new)> | proves it: <what the check or a test shows>
Each step changes at most 2 files and leaves the project building. Tests are a step of their own, after the
code they test. Then list open questions. Stop there; I reply with OK to implement, and the steps run as a
chain of workers, one per step.
