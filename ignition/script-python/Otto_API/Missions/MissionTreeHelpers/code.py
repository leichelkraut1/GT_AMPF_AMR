from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import readTagValues


def _log():
    return system.util.getLogger("Otto_API.Missions.MissionTreeHelpers")


def _browseMissionInstancePaths(rootPath):
    """
    Return all mission UDT instance paths below the given folder.
    """
    missionPaths = []
    pending = [rootPath]

    while pending:
        currentFolder = pending.pop(0)
        try:
            browseResults = browseTagResults(currentFolder)
        except Exception as exc:
            _log().warn(
                "Failed to browse mission folder [{}]: {}".format(
                    currentFolder,
                    str(exc)
                )
            )
            continue

        for row in browseResults:
            tagType = str(row.get("tagType"))
            fullPath = str(row.get("fullPath"))
            if tagType == "UdtInstance":
                missionPaths.append(fullPath)
            elif tagType == "Folder":
                pending.append(fullPath)

    return missionPaths


def browseMissionInstances(rootPath):
    """
    Return all mission UDT instances as (fullPath, name) tuples.
    """
    return [
        (path, path.rsplit("/", 1)[1])
        for path in _browseMissionInstancePaths(rootPath)
    ]


def _pickValue(*qualifiedValues):
    for qualifiedValue in qualifiedValues:
        if qualifiedValue is None or not qualifiedValue.quality.isGood():
            continue

        value = qualifiedValue.value
        if value is None:
            continue

        if not str(value).strip():
            continue

        return value

    return None


def readMissionRobotAwareRecords(rootPath):
    """
    Read mission robot affinity fields plus id in one recursive browse + bulk read pass.
    """
    missionRows = []
    for missionBasePath, _ in browseMissionInstances(rootPath):
        missionRows.append({
            "assigned_robot_path": missionBasePath + "/assigned_robot",
            "assigned_robot_alt_path": missionBasePath + "/Assigned_Robot",
            "force_robot_path": missionBasePath + "/force_robot",
            "force_robot_alt_path": missionBasePath + "/Force_Robot",
            "forced_robot_path": missionBasePath + "/forced_robot",
            "forced_robot_alt_path": missionBasePath + "/Forced_Robot",
            "id_path": missionBasePath + "/id",
            "id_alt_path": missionBasePath + "/ID",
        })

    readPaths = []
    for missionRow in missionRows:
        readPaths.extend([
            missionRow["assigned_robot_path"],
            missionRow["assigned_robot_alt_path"],
            missionRow["force_robot_path"],
            missionRow["force_robot_alt_path"],
            missionRow["forced_robot_path"],
            missionRow["forced_robot_alt_path"],
            missionRow["id_path"],
            missionRow["id_alt_path"],
        ])

    readResults = readTagValues(readPaths)
    expectedReadCount = len(missionRows) * 8
    if len(readResults) < expectedReadCount:
        _log().warn(
            "Expected {} mission robot-affinity tag values under [{}] but received {}".format(
                expectedReadCount,
                rootPath,
                len(readResults)
            )
        )
        return []
    missionRecords = []

    for index, missionRow in enumerate(missionRows):
        offset = index * 8
        missionRecords.append({
            "assigned_robot": _pickValue(readResults[offset], readResults[offset + 1]),
            "force_robot": _pickValue(readResults[offset + 2], readResults[offset + 3]),
            "forced_robot": _pickValue(readResults[offset + 4], readResults[offset + 5]),
            "id": _pickValue(readResults[offset + 6], readResults[offset + 7]),
        })

    return missionRecords


def readMissionIdRecords(rootPath):
    """
    Read mission ids only in one recursive browse + bulk read pass.
    """
    missionRows = []
    for missionBasePath, _ in browseMissionInstances(rootPath):
        missionRows.append({
            "id_path": missionBasePath + "/id",
            "id_alt_path": missionBasePath + "/ID",
        })

    readPaths = []
    for missionRow in missionRows:
        readPaths.extend([
            missionRow["id_path"],
            missionRow["id_alt_path"],
        ])

    readResults = readTagValues(readPaths)
    expectedReadCount = len(missionRows) * 2
    if len(readResults) < expectedReadCount:
        _log().warn(
            "Expected {} mission id tag values under [{}] but received {}".format(
                expectedReadCount,
                rootPath,
                len(readResults)
            )
        )
        return []
    missionRecords = []

    for index, missionRow in enumerate(missionRows):
        offset = index * 2
        missionId = _pickValue(readResults[offset], readResults[offset + 1])
        if missionId:
            missionRecords.append({"id": missionId})

    return missionRecords
