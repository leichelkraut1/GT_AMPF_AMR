from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceText


class ContainerCreateFields(MappingRecordBase):
    FIELDS = ("id", "container_type", "empty", "description", "name", "place", "robot")

    def __init__(
        self,
        containerId,
        containerType,
        empty,
        description=None,
        name=None,
        place=None,
        robot=None,
    ):
        self.id = coerceText(containerId)
        self.container_type = coerceText(containerType)
        self.empty = coerceBool(empty, False)
        self.description = coerceText(description, None)
        self.name = coerceText(name, None)
        self.place = coerceText(place, None)
        self.robot = coerceText(robot, None)

    @classmethod
    def fromDict(cls, fields):
        if isinstance(fields, cls):
            return fields
        fields = dict(fields or {})
        return cls(
            fields.get("id"),
            fields.get("container_type"),
            fields.get("empty"),
            fields.get("description"),
            fields.get("name"),
            fields.get("place"),
            fields.get("robot"),
        )

    @classmethod
    def listFromDicts(cls, rows):
        return [cls.fromDict(row) for row in list(rows or [])]

    def toPayloadFields(self):
        payload = {}
        for fieldName in list(self.FIELDS or ()):
            value = getattr(self, fieldName)
            if value in [None, ""] and fieldName not in ["empty"]:
                continue
            payload[fieldName] = value
        return payload

    def withPlace(self, placeId):
        return self.cloneWith(place=placeId, robot=None)

    def withRobot(self, robotId):
        return self.cloneWith(place=None, robot=robotId)


class ContainerLocationTarget(MappingRecordBase):
    FIELDS = ("kind", "value")

    def __init__(self, kind, value):
        self.kind = str(coerceText(kind) or "").lower()
        self.value = coerceText(value, None)

    @classmethod
    def fromDict(cls, target):
        if isinstance(target, cls):
            return target
        target = dict(target or {})
        return cls(target.get("kind") or target.get("type"), target.get("value"))

    @classmethod
    def fromKindValue(cls, kind, value):
        if isinstance(kind, cls):
            return kind
        if str(kind or "").strip().lower() == "robot":
            return cls.forRobot(value)
        return cls.forPlace(value)

    @classmethod
    def forPlace(cls, placeId):
        return cls("place", placeId)

    @classmethod
    def forRobot(cls, robotId):
        return cls("robot", robotId)

    def isPlace(self):
        return self.kind == "place"

    def isRobot(self):
        return self.kind == "robot"
