---
description: Scout gathers context, planner plans, worker implements (handoff files, narrow scopes)
---
Use the subagent tool in chain mode for: $@

1. scout: goal = find the code relevant to the task above; read-only.
2. planner: goal = an implementation plan for the task, listing the exact files to change; read-only.
3. worker: goal = implement the plan; scope = ["@previous"] (the plan's FILES line); done_when = the project's check or test command, if there is one.

Each step gets the previous report automatically. If a step returns NEEDS, handle it as your guidelines say, resume the run, and continue with the remaining steps.
