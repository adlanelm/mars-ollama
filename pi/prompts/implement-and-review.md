---
description: Worker implements, reviewer reviews, worker applies the feedback
---
Use the subagent tool in chain mode for: $@

1. worker: goal = implement the task above; scope = only the files it needs to change (at most 2, by name; split larger tasks with /implement); done_when = what the finished change does, in one line (the project check runs by itself).
2. reviewer: goal = review the worker's change (its report lists the files); read-only.
3. worker: goal = apply the reviewer's feedback; scope = ["@previous"] (the files step 1 changed).

Each step gets the previous report automatically. If a step returns NEEDS, handle it as your guidelines say, resume the run, and continue.
