---
description: Scout and planner produce small steps, then one worker per step (handoff files, narrow scopes)
---
[workers-edit] Implement: $@

Your tools here: read (project docs such as GAME.md and BACKLOG.md, and subagent reports) and subagent. You do not
look at code: everything about the code comes from the scout.

1. Use the subagent tool in chain mode: scout, then planner (numbered steps, each changing at most 2 files and
   leaving the project building). The scout's goal is 2-4 specific questions the plan depends on (which class
   does X and where, which method to extend, how the tests check Y), with path:line answers; read-only. Not
   "find the code relevant to the task": a broad brief makes a scout read for half an hour.
If the plan's Design section starts with `DESIGN CHANGE:`, ask the user before running any worker (in a
   non-interactive run, continue and put the design change first in your final summary).
2. Read the plan report, then run its steps as a second chain: one worker per step, in order, with
   scope = exactly that step's files (the call's `scope` parameter; files named only in the goal are not writable) and goal = that step's line plus the context the worker needs.

The project checks run automatically after each step and at the end; do not tell workers to run them. If a step returns NEEDS, handle it as your
guidelines say, resume the run, and continue with the remaining steps.
