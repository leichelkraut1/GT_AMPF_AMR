from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Maps.Get import updateMaps
from Otto_API.Places.Get import updatePlaces
from Otto_API.Robots.Get import updateRobotOperationalState
from Otto_API.Robots.Get import updateRobots
from Otto_API.TriggerHelpers import ensureMissionTriggerTags
from Otto_API.Workflows.Get import updateWorkflows

from MainController.WorkflowConfig import getWorkflowDefs
from MainController.WorkflowConfig import ROBOT_NAMES


def _log():
    return system.util.getLogger("Otto_API.Fleet.Refresh")


def _robotIds():
    ids = []
    for robotName in list(ROBOT_NAMES or []):
        text = str(robotName or "").strip()
        if "RV" not in text:
            continue
        suffix = text.rsplit("RV", 1)[-1]
        try:
            ids.append(int(suffix))
        except Exception:
            continue
    return sorted(list(set(ids)))


def _workflowIds():
    workflowDefs = dict(getWorkflowDefs() or {})
    ids = []
    for workflowNumber in list(workflowDefs.keys()):
        try:
            ids.append(int(workflowNumber))
        except Exception:
            continue
    return sorted(list(set(ids)))


def refreshFleetData():
    """
    Refresh fleet-side sync surfaces in dependency order.

    Order:
    - provision/update triggers
    - maps (which also updates ActiveMapID)
    - places
    - robot inventory
    - robot operational state
    - workflows
    """
    logger = _log()
    workflowIds = _workflowIds()
    robotIds = _robotIds()

    logger.info(
        "Otto API - Refreshing fleet data (workflowIds={}, robotIds={})".format(
            workflowIds,
            robotIds,
        )
    )

    triggerPaths = ensureMissionTriggerTags(
        workflowIds=workflowIds or None,
        robotIds=robotIds or None,
    )
    triggerResult = buildOperationResult(
        True,
        "info",
        "Mission and container triggers refreshed",
        data={"created_paths": list(triggerPaths or [])},
        created_paths=list(triggerPaths or []),
    )

    mapResult = updateMaps()
    placeResult = updatePlaces()
    robotInventoryResult = updateRobots()
    robotStateResult = updateRobotOperationalState()
    workflowResult = updateWorkflows()

    orderedResults = [
        ("triggers", triggerResult),
        ("maps", mapResult),
        ("places", placeResult),
        ("robot_inventory", robotInventoryResult),
        ("robot_state", robotStateResult),
        ("workflows", workflowResult),
    ]

    hasError = False
    hasWarn = False
    for _name, result in list(orderedResults or []):
        level = str(dict(result or {}).get("level") or "").lower()
        ok = bool(dict(result or {}).get("ok"))
        if level == "error":
            hasError = True
        elif level == "warn" or not ok:
            hasWarn = True

    level = "info"
    if hasError:
        level = "error"
    elif hasWarn:
        level = "warn"

    ok = not hasError and not hasWarn
    message = "Fleet data refresh completed"
    if hasError:
        message = "Fleet data refresh completed with errors"
    elif hasWarn:
        message = "Fleet data refresh completed with warnings"

    return buildOperationResult(
        ok,
        level,
        message,
        data={"results": dict(orderedResults)},
        results=dict(orderedResults),
    )
