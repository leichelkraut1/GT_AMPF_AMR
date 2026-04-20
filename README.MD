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

1. `Get.readCachedServerStatus()`
2. `Get.updateRobotOperationalState()`
3. `MissionSorting.run()`
4. `runAllRobotWorkflowCycles(...)`

The important rule is:

- PLC command evaluation only happens after robot state and fast mission state have
  been refreshed in the same cycle.

If robot or mission state is stale, the workflow phase is skipped and the PLC is told
that control is unhealthy.

The live OTTO server-status HTTP call is no longer part of the 1-second loop.
It is refreshed by the slower `OttoAPI_GetServerStatus` timer and read from the
cached `Fleet/System/ServerStatus` tag during the fast controller cycle.

## MainController Module Layout

The `MainController` code is split by responsibility:

- `CommandCalls`
  - top-level cycle entrypoints
- `CommandHelpers`
  - shared paths, tag reads/writes, runtime datasets, state normalization
- `RobotCycle`
  - per-robot orchestration
- `MissionCommandHelpers`
  - mission-level clear command issue/log/dedupe helpers
- `RobotDecisions`
  - small pure decision helpers
- `RobotActions`
  - create / cancel / finalize calls into the mission layer
- `PlcMirror`
  - shapes PLC output values for one robot

If you are troubleshooting one robot’s behavior, start in:

- `ignition/script-python/MainController/RobotCycle/code.py`

If you are troubleshooting explicit mission cancel/finalize dedupe or command-history
rows for clear actions, the next stop is:

- `ignition/script-python/MainController/MissionCommandHelpers/code.py`

## Command Structure

The controller runs one workflow cycle per robot.

The per-robot entrypoint is:

- `runRobotWorkflowCycle(robotName, ...)`

The logic is organized in this order:

1. Health gating
2. Reconcile the current `Active` mission set against the requested workflow
3. Validate create candidates once the `Active` folder is clear
4. Hold existing request/mission states when appropriate
5. Create a new mission if no hold path applies

This means the controller always prefers reconciliation and stability before sending
new actions.

When more than one mission exists in `Fleet/Missions/Active/<Robot>`, the controller
still chooses one "current mission" for reporting and request comparison using this
priority:

- `STARVED`
- executing-style statuses such as `EXECUTING`, `ASSIGNED`, `BLOCKED`,
  `CANCELLING`, `REASSIGNED`, `RESTARTING`
- `QUEUED`

## State Handling Model

Per-robot controller state lives under:

- `MainControl/Robots/<Robot>/...`

One important operator override now lives there too:

- `DisableIgnitionControl`
  - when `true`, the workflow runner still evaluates and mirrors state, but it will
    not create, cancel, or finalize OTTO missions for that robot

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

The per-robot `State` tag is intentionally compact now. It should only be one of:

- `idle`
- `mission_requested`
- `mission_active`
- `fault`

Detailed meaning lives in:

- `Action`
- `Level`
- `LastResult`
- PLC-facing booleans like `MissionNeedsFinalized`, `RequestInvalid`,
  `RequestConflict`, and `RequestRobotNotReady`

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
  - anti-repeat: keep the controller in `mission_requested` or `mission_active`
    and do not retrigger create

- Queued sidecar cleanup
  - evaluate: an `Active` mission is `QUEUED` and its workflow number does not match the PLC request
  - action: `cancel_mission` immediately, without waiting for `FinalizeOk`
  - anti-repeat: mission-level issued-command signature prevents repeating the same cancel every scan once it succeeds

- Finalize / cancel clear path
  - evaluate: at least one non-queued `Active` mission does not match the PLC request or the request is `0`
  - action: wait for PLC `FinalizeOk`, then reconcile the whole mismatched active set:
    `finalize_mission` for `STARVED`, `cancel_mission` for everything else
  - anti-repeat: mission-level issued-command signature prevents repeating the same clear command every scan once it succeeds

## Anti-Repeat Rules That Matter

These are the most important anti-repeat behaviors in the current design:

- `RequestLatched`
  - prevents create from firing every scan for the same request

- `MissionCreated`
  - tracks whether the controller already owns a created mission for the request cycle

- `MissionNeedsFinalized`
  - means at least one non-queued active mission still needs PLC-authorized clearing
    before a new request can take over

- `LastLoggedSignature`
  - prevents `CommandHistory` from getting the same row every scan for the same robot

- api_Mission runtime helper members
  - `_LastLoggedStatus`
  - `_LastWriteSignature`
  - `_LastIssuedCommandSignature`
  - `_LastCommandLogSignature`
  - let mission-state logging, mission writes, and mission-level clear commands all
    dedupe cleanly while the mission remains in `Fleet/Missions/...`
  - if one of these members is missing from the UDT, the gateway warns once and the
    corresponding dedupe behavior degrades safely until the UDT is updated

## Retry Behavior

The controller now keeps per-robot retry delay only for create attempts.

What happens today:

- Create
  - if create fails, the controller records a retry window before another create
    attempt is allowed
  - if the request still evaluates as create on the next cycle but the retry window
    is still active, the controller holds in a backoff state instead of sending
    create again

- Finalize / cancel clear commands
  - there is no robot-level retry window anymore
  - the controller reevaluates the `Active` mission set every scan
  - successful mission clear commands are suppressed by mission-level issued-command
    signatures
  - failed mission clear commands can retry on later scans because no issued signature
    is written on failure

So the short version is:

- create has a delay/backoff
- mission clear commands use mission-level anti-repeat instead of robot-level backoff
- retry state is tracked with:
  - `LastAttemptAction`
  - `NextActionAllowedEpochMs`
  - `RetryCount`
- after the cleanup, `LastAttemptAction` should only be meaningful for create
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

## Mission Sorting Split

Mission sorting is now intentionally split into:

- `MissionSorting.run()`
  - fast path
  - fetches `ACTIVE` and `FAILED`
  - updates active/failed mission tags
  - updates robot mission facts used by readiness and `MainController`
  - removes stale `Active`
  - keeps failed cleanup/retention in the fast path because `FailedMissionCount`
    is part of robot readiness

- `MissionSorting.runTerminalMaintenance()`
  - slow path
  - fetches `COMPLETED`
  - updates completed mission tags
  - removes stale `Completed`
  - applies completed retention pruning

The slow maintenance pass is driven by the `OttoAPI_UpdateMissions` timer.
This keeps the fast loop focused on state that affects control decisions.

## Current Commissioning Checkpoint

Current known-good checkpoint before production validation:

- UDT-backed `MainControl/Robots/<Robot>` and `PLC/<Robot>` structures are in use
- workflow-number requests can create missions
- request reconciliation now cancels mismatched queued sidecars immediately
- non-queued mismatches wait for `FinalizeOk`, then finalize `STARVED` missions and
  cancel the rest
- command and mission history datasets are working with duplicate suppression
- server status and completed-mission maintenance have been moved off the fast loop

Current focus:

- verify the same active-set reconciliation behavior on the production gateway

## Command History

Command history lives at:

- `MainControl/Runtime/CommandHistory`

It is a controller-decision log, not a raw scan log.

Duplicate rows are suppressed per robot using:

- `LastLoggedSignature`

This keeps repeated actions like `waiting_available` from filling the dataset
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
