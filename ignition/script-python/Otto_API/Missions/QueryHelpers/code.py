def _normalizeMissionStatusList(missionStatus):
    if missionStatus is None:
        return []

    if isinstance(missionStatus, (list, tuple)):
        values = missionStatus
    else:
        values = [missionStatus]

    normalized = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized.append(text)
    return normalized


def buildMissionsUrl(baseUrl, missionStatus, limit=None, offset=None):
    """
    Build the OTTO missions URL for one or more mission status filters.
    """
    statuses = _normalizeMissionStatusList(missionStatus)
    if not statuses:
        raise ValueError("At least one mission status is required")

    url = baseUrl + "/missions/?fields=%2A"
    if offset is not None:
        url += "&offset=" + str(offset)

    for status in statuses:
        url += "&mission_status=" + status

    if limit is not None:
        url += "&limit=" + str(limit)

    return url
