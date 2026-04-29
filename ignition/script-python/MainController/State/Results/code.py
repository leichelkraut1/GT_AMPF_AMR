from Otto_API.Models.Results import OperationalResult


def buildCycleResult(ok, level, message, robotName=None, state=None, action=None, data=None):
    """Wrap one robot-cycle decision in a consistent result payload."""
    payload = {
        "robot_name": robotName,
        "state": state,
        "action": action,
    }
    if data:
        payload.update(data)

    return OperationalResult(
        ok,
        level,
        message,
        dataFields=payload,
        topLevelFields={
            "robot_name": robotName,
            "state": state,
            "action": action,
        },
    ).toDict()
