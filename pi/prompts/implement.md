---
description: Scout and planner produce small steps, then one worker per step (handoff files, narrow scopes)
---
Implement: $@

1. Use the subagent tool in chain mode: scout (find the code relevant to the task; read-only), then planner
   (numbered steps, each changing at most 2 files and leaving the project building).
2. Read the plan report, then run its steps as a second chain: one worker per step, in order, with
   scope = exactly that step's files and goal = that step's line plus the context the worker needs.

The project checks run automatically after each step and at the end. If a step returns NEEDS, handle it as your
guidelines say, resume the run, and continue with the remaining steps.
