# GT_AMPF_AMR Programmer Notes

This project now has two main layers that matter for AMR mission control:

- `Otto_API`
  - syncs OTTO state into Ignition
  - sorts missions into `Fleet/Missions/...`
  - derives robot-facing mission facts such as `MissionStarved`
- `MainController`
  - reads PLC demand
  - evaluates robot state against the current mission state
  - creates, cancels, and finalizes missions
  - mirrors controller status back to the PLC

This file is meant to help the next programmer understand the command flow without
having to reconstruct it from a long chain of script modules.

## Main Loop

Top-level orchestration lives in:

- `ignition/script-python/MainController/CommandCalls/code.py`

The main entrypoint is:

- `runMainControllerCycle(...)`

The cycle order is intentional:

1. `Get.getServerStatus()`
2. `Get.updateRobotOperationalState()`
3. `MissionSorting.run()`
4. `runAllRobotWorkflowCycles(...)`

The important rule is:

- PLC command evaluation only happens after robot state and mission state have been
  refreshed in the same cycle.

If robot or mission state is stale, the workflow phase is skipped and the PLC is told
that control is unhealthy.

## MainController Module Layout

The `MainController` code is split by responsibility:

- `CommandCalls`
  - top-level cycle entrypoints
- `CommandHelpers`
  - shared paths, tag reads/writes, runtime datasets, state normalization
- `RobotCycle`
  - per-robot orchestration
- `RobotDecisions`
  - pure decision helpers and stable message builders
- `RobotActions`
  - create / cancel / finalize calls into the mission layer
- `PlcMirror`
  - shapes PLC output values for one robot

If you are troubleshooting one robot’s behavior, start in:

- `ignition/script-python/MainController/RobotCycle/code.py`

## Command Structure

The controller runs one workflow cycle per robot.

The per-robot entrypoint is:

- `runRobotWorkflowCycle(robotName, ...)`

The logic is organized in this order:

1. Health gating
2. Reconcile any robot that already owes a finalize or cancel outcome
3. Handle "no requested workflow"
4. Validate new or changed workflow requests
5. Hold existing request/mission states when appropriate
6. Create a new mission if no hold path applies

This means the controller always prefers reconciliation and stability before sending
new actions.

## State Handling Model

Per-robot controller state lives under:

- `MainControl/Robots/<Robot>/...`

PLC interface tags live under:

- `PLC/<Robot>/FromPLC/...`
- `PLC/<Robot>/ToPLC/...`

The robot cycle uses this pattern:

1. Read current state from tags
2. Read PLC inputs and mirrored fleet/mission facts
3. Build one mutable `nextState`
4. Evaluate the branch
5. Mutate `nextState`
6. Perform the action if needed
7. Write the final state once through `_finishCycle(...)`

That "build once, mutate, finish once" structure is deliberate. It avoids the old
pattern where every branch rebuilt nearly the same state patch over and over.

## Evaluation, Then Action, Then Anti-Repeat

The easiest way to reason about the controller is:

1. Evaluate current conditions
2. Take the action for the chosen branch
3. Latch state so the same command is not repeated blindly

Examples:

- Create path
  - evaluate: requested workflow is valid, not already active, robot is available
  - action: `callCreateMission(...)`
  - anti-repeat: set `RequestLatched = true` and `State = mission_requested`

- Hold path
  - evaluate: same requested workflow is still latched or already active
  - action: no OTTO side effect
  - anti-repeat: keep the controller in a hold state and do not retrigger create

- Finalize path
  - evaluate: `MissionNeedsFinalized`, PLC `FinalizeOk`, and `MissionStarved`
  - action: `callFinalizeMission(...)`
  - anti-repeat: clear finalize state on success

- Switch path
  - evaluate: requested workflow changed while another one is active
  - action: cancel current mission if PLC allows the switch
  - anti-repeat: move into `switch_cancel_requested` until the old mission clears

## Anti-Repeat Rules That Matter

These are the most important anti-repeat behaviors in the current design:

- `RequestLatched`
  - prevents create from firing every scan for the same request

- `MissionCreated`
  - tracks whether the controller already owns a created mission for the request cycle

- `MissionNeedsFinalized`
  - means the robot is in reconciliation mode and should not be treated like a clean
    new request

- `LastLoggedSignature`
  - prevents `CommandHistory` from getting the same row every scan for the same robot

## Retry Behavior

The controller now uses a per-robot retry delay for failed actions.

What happens today:

- Create
  - if create fails, the controller records a retry window before another create
    attempt is allowed
  - if the request still evaluates as create on the next cycle but the retry window
    is still active, the controller holds in a backoff state instead of sending
    create again

- Finalize
  - if finalize fails, the robot remains in a finalize-needed condition and records
    a finalize retry window
  - the same finalize action is not retried again until that window expires

- Cancel
  - if cancel fails, the controller records a cancel retry window
  - the same cancel action is not retried again until that window expires

So the short version is:

- retries have a delay/backoff
- the backoff only applies to retries of the same action
- if the evaluated action changes, the old retry state is cleared
- retry state is tracked with:
  - `LastAttemptAction`
  - `NextActionAllowedEpochMs`
  - `RetryCount`
- the current shared retry delay is controlled by
  `MainController.CommandHelpers.RETRY_DELAY_MS`

This keeps a failed create from being hammered every scan, while still allowing a
later cancel or finalize branch to proceed immediately if the situation changes.

## Mission State Logging

Mission state history lives at:

- `MainControl/Runtime/MissionStateHistory`

This log tracks OTTO mission status changes, not folder bucket changes.

Examples:

- `"" -> QUEUED`
- `QUEUED -> EXECUTING`
- `STARVED -> SUCCEEDED`
- `QUEUED -> CANCELLED`

The `Completed` / `Failed` / `Active` mission folders are only storage buckets.
They are not what gets logged in the mission state history.

## Command History

Command history lives at:

- `MainControl/Runtime/CommandHistory`

It is a controller-decision log, not a raw scan log.

Duplicate rows are suppressed per robot using:

- `LastLoggedSignature`

This keeps repeated hold states like `waiting_available` from filling the dataset
every loop.

## Soft Failures vs Hard Failures

In `runAllRobotWorkflowCycles(...)`, per-robot `warn` results are treated as soft
failures in the aggregate result.

That means states like:

- waiting for availability
- request conflict
- invalid request

do not make the whole workflow pass look like a hard controller failure.

Only real execution problems should drive the aggregate result hard-failed.

## Ignition Resource Reminder

When adding a new script module under:

- `ignition/script-python/...`

remember to add:

- `resource.json`

in the same folder.

Without it, the local repo may look fine but the Ignition gateway may not recognize
the new module and imports will fail at runtime.
