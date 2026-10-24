---
description: Scout and planner produce small steps, then one worker per step (handoff files, narrow scopes)
---
Implement: $@

1. Use the subagent tool in chain mode: scout, then planner (numbered steps, each changing at most 2 files and
   leaving the project building). The scout's goal is 2-4 specific questions the plan depends on (which class
   does X and where, which method to extend, how the tests check Y), with path:line answers; read-only. Not
   "find the code relevant to the task": a broad brief makes a scout read for half an hour.
2. Read the plan report, then run its steps as a second chain: one worker per step, in order, with
   scope = exactly that step's files and goal = that step's line plus the context the worker needs.

The project checks run automatically after each step and at the end. If a step returns NEEDS, handle it as your
guidelines say, resume the run, and continue with the remaining steps.
