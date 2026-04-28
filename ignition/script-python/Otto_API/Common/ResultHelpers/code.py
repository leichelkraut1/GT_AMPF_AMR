def buildOperationResult(ok, level, message, data=None, **extra):
    """
    Build a normalized operation result object.

    Common contract:
    - ok
    - level
    - message
    - data

    Module-specific fields may still be included through extra kwargs.
    """
    result = {
        "ok": ok,
        "level": level,
        "message": message,
        "data": data,
    }
    result.update(extra)
    return result


def _typedValueToData(value):
    if hasattr(value, "toDict"):
        return value.toDict()
    if isinstance(value, list) or isinstance(value, tuple):
        return [_typedValueToData(item) for item in value]
    if isinstance(value, dict):
        return dict(
            (key, _typedValueToData(item))
            for key, item in dict(value).items()
        )
    return value


def buildTypedOperationResult(
    ok,
    level,
    message,
    typedFields=None,
    dataFields=None,
    sharedFields=None,
):
    typedFields = dict(typedFields or {})
    dataFields = dict(dataFields or {})
    sharedFields = dict(sharedFields or {})
    resultFields = dict(typedFields)
    dataPayload = {}

    for fieldName, value in typedFields.items():
        dataPayload[fieldName] = _typedValueToData(value)

    for fieldName, value in dataFields.items():
        dataPayload[fieldName] = _typedValueToData(value)

    for fieldName, value in sharedFields.items():
        dataPayload[fieldName] = _typedValueToData(value)

    resultFields.update(sharedFields)
    return buildOperationResult(
        ok,
        level,
        message,
        data=dataPayload,
        **resultFields
    )


def buildRecordSyncResult(
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
    typedFields = {
        "records": list(records or []),
        "writes": list(writes or []),
        "records_by_name": dict(recordsByName or {}),
    }
    dataFields = dict(dataFields or {})
    dataFields["value"] = value
    return buildTypedOperationResult(
        ok,
        level,
        message,
        typedFields=typedFields,
        dataFields=dataFields,
        sharedFields=sharedFields,
    )
