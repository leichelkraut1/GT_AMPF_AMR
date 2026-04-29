def _typedValueToData(value):
    if hasattr(value, "toDict"):
        return value.toDict()
    if isinstance(value, list) or isinstance(value, tuple):
        return [_typedValueToData(item) for item in list(value or [])]
    if isinstance(value, dict):
        return dict(
            (key, _typedValueToData(item))
            for key, item in dict(value or {}).items()
        )
    return value


class OperationHealth(object):
    def __init__(self, ok, level, message, warnings=None, issues=None):
        self.ok = bool(ok)
        self.level = str(level or "").strip()
        self.message = str(message or "")
        self.warnings = list(warnings or [])
        self.issues = list(issues or [])

    def isError(self):
        return str(self.level or "").lower() == "error"

    def isWarn(self):
        return str(self.level or "").lower() == "warn"

    def isHealthy(self):
        return bool(self.ok)

    def healthDict(self):
        return {
            "ok": self.ok,
            "level": self.level,
            "message": self.message,
            "warnings": _typedValueToData(self.warnings),
            "issues": _typedValueToData(self.issues),
        }


class OperationalResult(OperationHealth):
    def __init__(
        self,
        ok,
        level,
        message,
        typedFields=None,
        dataFields=None,
        sharedFields=None,
    ):
        OperationHealth.__init__(self, ok, level, message)
        self._typed_field_names = []
        self._data_field_names = []
        self._shared_field_names = []

        typedFields = dict(typedFields or {})
        dataFields = dict(dataFields or {})
        sharedFields = dict(sharedFields or {})

        dataPayload = {}
        for fieldName, value in list(typedFields.items()):
            setattr(self, fieldName, value)
            self._typed_field_names.append(fieldName)
            dataPayload[fieldName] = _typedValueToData(value)

        for fieldName, value in list(dataFields.items()):
            setattr(self, fieldName, value)
            self._data_field_names.append(fieldName)
            dataPayload[fieldName] = _typedValueToData(value)

        for fieldName, value in list(sharedFields.items()):
            setattr(self, fieldName, value)
            self._shared_field_names.append(fieldName)
            dataPayload[fieldName] = _typedValueToData(value)

        self.data = dataPayload

    @classmethod
    def fromDict(cls, result):
        if isinstance(result, cls):
            return result
        result = dict(result or {})
        return cls(
            result.get("ok"),
            result.get("level"),
            result.get("message"),
            dataFields=result.get("data"),
        )

    def _serializedSharedFields(self):
        fields = {}
        fieldNames = list(self._typed_field_names or [])
        fieldNames.extend(list(self._shared_field_names or []))
        for fieldName in fieldNames:
            fields[fieldName] = _typedValueToData(getattr(self, fieldName))
        return fields

    def toDict(self):
        result = {
            "ok": self.ok,
            "level": self.level,
            "message": self.message,
            "data": _typedValueToData(self.data),
        }
        result.update(self._serializedSharedFields())
        return result


class RecordSyncResult(OperationalResult):
    def __init__(
        self,
        ok,
        level,
        message,
        records=None,
        recordsByName=None,
        writes=None,
        value=None,
        dataFields=None,
        sharedFields=None,
    ):
        self.records = list(records or [])
        self.records_by_name = dict(recordsByName or {})
        self.writes = list(writes or [])
        self.value = value

        dataFields = dict(dataFields or {})
        dataFields["value"] = value

        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields={
                "records": self.records,
                "records_by_name": self.records_by_name,
                "writes": self.writes,
            },
            dataFields=dataFields,
            sharedFields=sharedFields,
        )
