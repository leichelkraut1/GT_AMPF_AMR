from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceInt
from Otto_API.Common.RecordHelpers import coerceIntOrNone
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Common.SyncHelpers import sanitizeTagName


def buildInterlockInstanceName(rawName):
    """
    Convert an OTTO interlock name into a safe Ignition tag instance name.
    """
    return sanitizeTagName(rawName)


def buildInterlockInstanceMap(records):
    """
    Build a raw-name -> sanitized-instance-name map and detect collisions.
    """
    instanceNameByRawName = {}
    rawNameByInstanceName = {}
    errors = []

    for record in list(records or []):
        if isinstance(record, dict):
            rawName = str(record.get("name") or "").strip()
        else:
            rawName = str(getattr(record, "name", "") or "").strip()
        if not rawName:
            continue

        instanceName = buildInterlockInstanceName(rawName)
        existingRawName = rawNameByInstanceName.get(instanceName)
        if existingRawName is not None and existingRawName != rawName:
            errors.append(
                "Interlock names [{}] and [{}] both map to Fleet/Interlocks/[{}]".format(
                    existingRawName,
                    rawName,
                    instanceName,
                )
            )
            continue

        rawNameByInstanceName[instanceName] = rawName
        instanceNameByRawName[rawName] = instanceName

    return instanceNameByRawName, rawNameByInstanceName, errors


class InterlockRecord(MappingRecordBase):
    FIELDS = ("id", "created", "name", "state")

    def __init__(self, interlockId, created, name, state):
        self.id = coerceText(interlockId)
        self.created = coerceText(created)
        self.name = coerceText(name)
        self.state = coerceInt(state, 0)

    @classmethod
    def fromDict(cls, rawRecord):
        if isinstance(rawRecord, cls):
            return rawRecord
        rawRecord = dict(rawRecord or {})
        return cls(
            rawRecord.get("id"),
            rawRecord.get("created"),
            rawRecord.get("name"),
            rawRecord.get("state", 0),
        )

    @classmethod
    def listFromDicts(cls, rawRecords):
        return [cls.fromDict(rawRecord) for rawRecord in list(rawRecords or [])]

    def hasValidId(self):
        return bool(self.id and self.name)


class InterlockMappingRow(MappingRecordBase):
    FIELDS = ("FleetName", "PlcTagName", "Direction", "WriteEnable")

    def __init__(self, fleetName, plcTagName, direction, writeEnable=True):
        self.FleetName = coerceText(fleetName)
        self.PlcTagName = coerceText(plcTagName)
        self.Direction = coerceText(direction)
        self.WriteEnable = coerceBool(writeEnable, True)

    @classmethod
    def fromDict(cls, row):
        if isinstance(row, cls):
            return row
        row = dict(row or {})
        return cls(
            row.get("FleetName"),
            row.get("PlcTagName"),
            row.get("Direction"),
            row.get("WriteEnable", True),
        )

    @classmethod
    def listFromDicts(cls, rows):
        return [cls.fromDict(row) for row in list(rows or [])]

    def isFromFleet(self):
        return self.Direction == "FromFleet"

    def isToFleet(self):
        return self.Direction == "ToFleet"

    def isWritable(self):
        return bool(self.WriteEnable)


class DuplicateInterlockMappingInfo(MappingRecordBase):
    FIELDS = (
        "fleet_name",
        "replaced_plc_tag_name",
        "replaced_direction",
        "winning_plc_tag_name",
        "winning_direction",
    )

    def __init__(
        self,
        fleetName,
        replacedPlcTagName,
        replacedDirection,
        winningPlcTagName,
        winningDirection,
    ):
        self.fleet_name = coerceText(fleetName)
        self.replaced_plc_tag_name = coerceText(replacedPlcTagName)
        self.replaced_direction = coerceText(replacedDirection)
        self.winning_plc_tag_name = coerceText(winningPlcTagName)
        self.winning_direction = coerceText(winningDirection)

    @classmethod
    def fromDict(cls, row):
        if isinstance(row, cls):
            return row
        row = dict(row or {})
        return cls(
            row.get("fleet_name"),
            row.get("replaced_plc_tag_name"),
            row.get("replaced_direction"),
            row.get("winning_plc_tag_name"),
            row.get("winning_direction"),
        )

    @classmethod
    def listFromDicts(cls, rows):
        return [cls.fromDict(row) for row in list(rows or [])]


class PlcInterlockSnapshot(MappingRecordBase):
    FIELDS = ("plc_tag_name", "state", "force_zero")

    def __init__(self, plcTagName, state, forceZero=False):
        self.plc_tag_name = coerceText(plcTagName)
        self.state = coerceIntOrNone(state)
        self.force_zero = coerceBool(forceZero, False)

    @classmethod
    def fromValues(cls, plcTagName, state, forceZero):
        return cls(plcTagName, state, forceZero)

    def forceZeroActive(self):
        return bool(self.force_zero)
