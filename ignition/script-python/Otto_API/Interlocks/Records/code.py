from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceInt
from Otto_API.Common.RecordHelpers import coerceIntOrNone
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Interlocks.Helpers import normalizeBool


class InterlockRecord(MappingRecordBase):
    FIELDS = ("id", "created", "name", "state")

    def __init__(self, interlockId, created, name, state):
        self.id = coerceText(interlockId)
        self.created = coerceText(created)
        self.name = coerceText(name)
        self.state = coerceInt(state, 0)

    @classmethod
    def fromDict(cls, rawRecord):
        rawRecord = dict(rawRecord or {})
        return cls(
            rawRecord.get("id"),
            rawRecord.get("created"),
            rawRecord.get("name"),
            rawRecord.get("state", 0),
        )

    def hasValidId(self):
        return bool(self.id and self.name)


def isInterlockRecord(value):
    return isinstance(value, InterlockRecord)


def coerceInterlockRecord(record):
    if isInterlockRecord(record):
        return record
    return InterlockRecord.fromDict(record)


class InterlockMappingRow(MappingRecordBase):
    FIELDS = ("FleetName", "PlcTagName", "Direction", "WriteEnable")

    def __init__(self, fleetName, plcTagName, direction, writeEnable=True):
        self.FleetName = coerceText(fleetName)
        self.PlcTagName = coerceText(plcTagName)
        self.Direction = coerceText(direction)
        self.WriteEnable = normalizeBool(writeEnable, True)

    @classmethod
    def fromDict(cls, row):
        row = dict(row or {})
        return cls(
            row.get("FleetName"),
            row.get("PlcTagName"),
            row.get("Direction"),
            row.get("WriteEnable", True),
        )

    def isFromFleet(self):
        return self.Direction == "FromFleet"

    def isToFleet(self):
        return self.Direction == "ToFleet"

    def isWritable(self):
        return bool(self.WriteEnable)


def isInterlockMappingRow(value):
    return isinstance(value, InterlockMappingRow)


def coerceInterlockMappingRow(row):
    if isInterlockMappingRow(row):
        return row
    return InterlockMappingRow.fromDict(row)


class DuplicateInterlockMappingInfo(MappingRecordBase):
    FIELDS = (
        "fleet_name",
        "replaced_plc_tag_name",
        "replaced_direction",
        "winning_plc_tag_name",
        "winning_direction",
    )

    def __init__(self, fleetName, replacedPlcTagName, replacedDirection, winningPlcTagName, winningDirection):
        self.fleet_name = coerceText(fleetName)
        self.replaced_plc_tag_name = coerceText(replacedPlcTagName)
        self.replaced_direction = coerceText(replacedDirection)
        self.winning_plc_tag_name = coerceText(winningPlcTagName)
        self.winning_direction = coerceText(winningDirection)

    @classmethod
    def fromDict(cls, row):
        row = dict(row or {})
        return cls(
            row.get("fleet_name"),
            row.get("replaced_plc_tag_name"),
            row.get("replaced_direction"),
            row.get("winning_plc_tag_name"),
            row.get("winning_direction"),
        )


def isDuplicateInterlockMappingInfo(value):
    return isinstance(value, DuplicateInterlockMappingInfo)


def coerceDuplicateInterlockMappingInfo(duplicateInfo):
    if isDuplicateInterlockMappingInfo(duplicateInfo):
        return duplicateInfo
    return DuplicateInterlockMappingInfo.fromDict(duplicateInfo)


class PlcInterlockSnapshot(MappingRecordBase):
    FIELDS = ("plc_tag_name", "state", "force_zero")

    def __init__(self, plcTagName, state, forceZero=False):
        self.plc_tag_name = coerceText(plcTagName)
        self.state = coerceIntOrNone(state)
        self.force_zero = normalizeBool(forceZero, False)

    @classmethod
    def fromValues(cls, plcTagName, state, forceZero):
        return cls(plcTagName, state, forceZero)

    def forceZeroActive(self):
        return bool(self.force_zero)
