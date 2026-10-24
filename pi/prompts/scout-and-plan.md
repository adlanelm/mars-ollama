---
description: Scout gathers context, planner writes a plan (no implementation)
---
[plan-only] Use the subagent tool in chain mode for: $@

1. scout: goal = 2-4 specific questions the plan depends on (which class does X and where, which method to
   extend, how the tests check Y), answered with path:line; read-only. Not "find the code relevant to the task".
2. planner: goal = an implementation plan for the task, listing the exact files to change; read-only.

Do NOT implement. Reply with the plan's report path and its SUMMARY line.
