from Otto_API.Models.Results import OperationHealth


class RobotCycleResult(OperationHealth):
    """Typed result for one robot workflow decision and apply pass."""

    def __init__(
        self,
        ok,
        level,
        message,
        robotName=None,
        state=None,
        action=None,
        data=None,
    ):
        OperationHealth.__init__(self, ok, level, message)
        self.robot_name = robotName
        self.state = state
        self.action = action
        self.data = dict(data or {})
        self.plc_sync_result = dict(self.data.get("plc_sync_result") or {})

    @classmethod
    def fromDict(cls, result):
        if isinstance(result, cls):
            return result
        if isinstance(result, OperationHealth):
            result = result.toDict()
        else:
            result = dict(result or {})
        data = dict(result.get("data") or {})
        return cls(
            result.get("ok"),
            result.get("level"),
            result.get("message"),
            robotName=data.get("robot_name", result.get("robot_name")),
            state=data.get("state", result.get("state")),
            action=data.get("action", result.get("action")),
            data=data,
        )

    def toDict(self):
        data = dict(self.data or {})
        data["robot_name"] = self.robot_name
        data["state"] = self.state
        data["action"] = self.action
        result = self.healthDict()
        result.pop("warnings", None)
        result.pop("issues", None)
        result["data"] = data
        return result
