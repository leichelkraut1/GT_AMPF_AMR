try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)


PROJECT_ROOT_TAG_PATH = "[Otto_FleetManager]"
FLEET_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "Fleet"
FLEET_SYSTEM_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/System"
FLEET_ROBOTS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Robots"
FLEET_MISSIONS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Missions"
FLEET_TRIGGERS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Triggers"
FLEET_PLACES_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Places"
FLEET_MAPS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Maps"
FLEET_WORKFLOWS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Workflows"
PLC_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "PLC"
MAINCONTROL_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "MainControl"
MAINCONTROL_INTERNAL_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Internal"
MAINCONTROL_ROBOTS_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Robots"
MAINCONTROL_RUNTIME_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Runtime"

API_BASE_URL_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Url_ApiBase"
SYSTEM_LAST_RESPONSE_TAG_PATH = FLEET_SYSTEM_ROOT_TAG_PATH + "/lastResponse"
MISSION_TRIGGER_LAST_RESPONSE_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/Triggers/lastResponse"
MISSION_MIN_CHARGE_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/minChargeLevelForMissioning"
MISSION_LAST_UPDATE_TS_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/LastUpdateTS"
MISSION_LAST_UPDATE_SUCCESS_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/LastUpdateSuccess"


def getProjectRootPath():
    return PROJECT_ROOT_TAG_PATH


def getFleetRootPath():
    return FLEET_ROOT_TAG_PATH


def getFleetSystemPath():
    return FLEET_SYSTEM_ROOT_TAG_PATH


def getFleetRobotsPath():
    return FLEET_ROBOTS_ROOT_TAG_PATH


def getFleetMissionsPath():
    return FLEET_MISSIONS_ROOT_TAG_PATH


def getFleetTriggersPath():
    return FLEET_TRIGGERS_ROOT_TAG_PATH


def getFleetPlacesPath():
    return FLEET_PLACES_ROOT_TAG_PATH


def getFleetMapsPath():
    return FLEET_MAPS_ROOT_TAG_PATH


def getFleetWorkflowsPath():
    return FLEET_WORKFLOWS_ROOT_TAG_PATH


def getPlcRootPath():
    return PLC_ROOT_TAG_PATH


def getMainControlRootPath():
    return MAINCONTROL_ROOT_TAG_PATH


def getMainControlInternalPath():
    return MAINCONTROL_INTERNAL_ROOT_TAG_PATH


def getMainControlRobotsPath():
    return MAINCONTROL_ROBOTS_ROOT_TAG_PATH


def getMainControlRuntimePath():
    return MAINCONTROL_RUNTIME_ROOT_TAG_PATH


def _normalizeTagLabel(label, tagPath):
    if label:
        return str(label)
    return "Tag"


def splitTagPath(path):
    """
    Split a full Ignition tag path into (parentPath, childName).
    """
    path = str(path)
    if "/" in path:
        return path.rsplit("/", 1)

    if "]" in path:
        providerPath, childName = path.split("]", 1)
        return providerPath + "]", childName

    raise ValueError("Unsupported tag path: {}".format(path))


def getApiBaseUrl():
    """
    Read the OTTO API base URL from the shared project tag.
    """
    return readRequiredTagValue(API_BASE_URL_TAG_PATH, "API base URL")


def getOttoOperationsUrl():
    """
    Build the OTTO operations endpoint from the shared base URL.
    """
    return getApiBaseUrl().rstrip("/") + "/operations/"


def getMissionMinChargePath():
    return MISSION_MIN_CHARGE_TAG_PATH


def getMissionLastUpdateTsPath():
    return MISSION_LAST_UPDATE_TS_TAG_PATH


def getMissionLastUpdateSuccessPath():
    return MISSION_LAST_UPDATE_SUCCESS_TAG_PATH


def getSystemLastResponsePath():
    return SYSTEM_LAST_RESPONSE_TAG_PATH


def getMissionTriggerLastResponsePath():
    return MISSION_TRIGGER_LAST_RESPONSE_TAG_PATH


def _validateTagRead(tagResult, tagPath, label, allowEmptyString=False):
    label = _normalizeTagLabel(label, tagPath)

    if not tagResult.quality.isGood():
        raise ValueError("{} is not readable: {}".format(label, tagPath))

    value = tagResult.value
    if value is None:
        raise ValueError("{} returned no value: {}".format(label, tagPath))

    if (
        not allowEmptyString
        and isinstance(value, string_types)
        and not value.strip()
    ):
        raise ValueError("{} returned an empty value: {}".format(label, tagPath))

    return value


def readRequiredTagValue(tagPath, label=None, allowEmptyString=False):
    """
    Read a required tag value and raise a clear ValueError if it is missing.
    """
    tagResult = readTagValues([tagPath])[0]
    return _validateTagRead(tagResult, tagPath, label, allowEmptyString)


def readTagValues(tagPaths):
    """
    Read multiple tag values synchronously.
    """
    tagPaths = list(tagPaths or [])
    if not tagPaths:
        return []
    return system.tag.readBlocking(tagPaths)


def readRequiredTagValues(tagPaths, labels=None, allowEmptyString=False):
    """
    Read multiple required tag values and raise clear ValueErrors for bad entries.
    """
    tagPaths = list(tagPaths or [])
    labels = list(labels or [])
    results = readTagValues(tagPaths)
    values = []

    for index, tagPath in enumerate(tagPaths):
        label = labels[index] if index < len(labels) else None
        values.append(
            _validateTagRead(results[index], tagPath, label, allowEmptyString)
        )

    return values


def readOptionalTagValues(tagPaths, defaultValues=None, allowEmptyString=False):
    """
    Read multiple optional tag values and substitute defaults for bad entries.
    """
    tagPaths = list(tagPaths or [])
    results = readTagValues(tagPaths)

    if defaultValues is None:
        defaultValues = []
    elif isinstance(defaultValues, (list, tuple)):
        defaultValues = list(defaultValues)
    else:
        defaultValues = [defaultValues] * len(tagPaths)

    values = []
    for index, tagPath in enumerate(tagPaths):
        defaultValue = defaultValues[index] if index < len(defaultValues) else None
        tagResult = results[index]

        if not tagResult.quality.isGood():
            values.append(defaultValue)
            continue

        value = tagResult.value
        if value is None:
            values.append(defaultValue)
            continue

        if (
            not allowEmptyString
            and isinstance(value, string_types)
            and not value.strip()
        ):
            values.append(defaultValue)
            continue

        values.append(value)

    return values


def readOptionalTagValue(tagPath, defaultValue=None, allowEmptyString=False):
    """
    Read an optional tag value and return the supplied default when missing.
    """
    tagResult = readTagValues([tagPath])[0]
    if not tagResult.quality.isGood():
        return defaultValue

    value = tagResult.value
    if value is None:
        return defaultValue

    if (
        not allowEmptyString
        and isinstance(value, string_types)
        and not value.strip()
    ):
        return defaultValue

    return value


def configureTagDefinitions(parentPath, tagDefs, collisionPolicy="i"):
    """
    Configure one or more tag definitions under the given parent path.
    """
    return system.tag.configure(parentPath, list(tagDefs), collisionPolicy)


def browseTagResults(path):
    """
    Browse a tag path and return its result rows.
    """
    return system.tag.browse(path).getResults()


def tagExists(path):
    """
    Return True when the given tag path exists.
    """
    return system.tag.exists(path)


def ensureFolder(path):
    """
    Ensure a tag folder exists at the given full path.
    """
    parentPath, name = splitTagPath(path)
    return configureTagDefinitions(
        parentPath,
        [{"name": name, "tagType": "Folder"}],
        "i"
    )


def ensureUdtInstance(parentPath, name, typeId, collisionPolicy="i"):
    """
    Ensure a UDT instance exists under the given parent folder.
    """
    ensureFolder(parentPath)
    return configureTagDefinitions(
        parentPath,
        [{
            "name": name,
            "typeID": typeId,
            "tagType": "UdtInstance",
        }],
        collisionPolicy
    )


def ensureUdtInstancePath(path, typeId, collisionPolicy="i"):
    """
    Ensure a UDT instance exists at the given full path.
    """
    parentPath, name = splitTagPath(path)
    return ensureUdtInstance(parentPath, name, typeId, collisionPolicy)


def ensureMemoryTag(path, dataType, initialValue=None, collisionPolicy="i"):
    """
    Ensure a memory-backed atomic tag exists at the given full path.
    """
    parentPath, name = splitTagPath(path)
    existed = tagExists(path)
    ensureFolder(parentPath)
    tagDef = {
        "name": name,
        "tagType": "AtomicTag",
        "valueSource": "memory",
        "dataType": dataType,
    }
    if initialValue is not None:
        tagDef["value"] = initialValue

    result = configureTagDefinitions(parentPath, [tagDef], collisionPolicy)
    if initialValue is not None and not existed:
        writeTagValue(path, initialValue)
    return result


def deleteTagPaths(tagPaths):
    """
    Delete one or more tag paths.
    """
    return system.tag.deleteTags(list(tagPaths))


def deleteTagPath(tagPath):
    """
    Delete a single tag path.
    """
    return deleteTagPaths([tagPath])


def _isWriteResultGood(result):
    if hasattr(result, "isGood"):
        return bool(result.isGood())

    if isinstance(result, dict):
        quality = result.get("quality")
        if hasattr(quality, "isGood"):
            return bool(quality.isGood())
        return str(quality).strip().lower() == "good"

    try:
        return int(result) == 0
    except Exception:
        return bool(result)


def _buildWriteFailureMessage(tagPath, result, label=None):
    if label:
        subject = str(label)
    else:
        subject = "Tag write"
    return "{} failed for {}: {}".format(subject, tagPath, result)


def writeTagValue(tagPath, value):
    """
    Write a single tag value synchronously.
    """
    return system.tag.writeBlocking([tagPath], [value])


def writeTagValues(tagPaths, values):
    """
    Write multiple tag values synchronously.
    """
    return system.tag.writeBlocking(list(tagPaths), list(values))


def writeRequiredTagValue(tagPath, value, label=None):
    """
    Write a required tag value and raise when the write result is not good.
    """
    results = writeTagValue(tagPath, value)
    result = results[0] if results else None
    if not _isWriteResultGood(result):
        raise ValueError(_buildWriteFailureMessage(tagPath, result, label))
    return results


def writeRequiredTagValues(tagPaths, values, labels=None):
    """
    Write required tag values and raise when any write result is not good.
    """
    tagPaths = list(tagPaths or [])
    values = list(values or [])
    labels = list(labels or [])
    results = writeTagValues(tagPaths, values)

    for index, tagPath in enumerate(tagPaths):
        result = results[index] if index < len(results) else None
        label = labels[index] if index < len(labels) else None
        if not _isWriteResultGood(result):
            raise ValueError(_buildWriteFailureMessage(tagPath, result, label))

    return results


def writeTagValueAsync(tagPath, value):
    """
    Write a single tag value asynchronously.
    """
    return system.tag.writeAsync(tagPath, value)


def writeTagValuesAsync(tagPaths, values):
    """
    Write multiple tag values asynchronously.
    """
    return system.tag.writeAsync(list(tagPaths), list(values))


def writeLastSystemResponse(value, asyncWrite=False):
    """
    Write the shared system lastResponse tag.
    """
    if asyncWrite:
        return writeTagValueAsync(getSystemLastResponsePath(), value)
    return writeTagValue(getSystemLastResponsePath(), value)


def writeLastTriggerResponse(value, asyncWrite=False):
    """
    Write the shared mission trigger lastResponse tag.
    """
    if asyncWrite:
        return writeTagValueAsync(getMissionTriggerLastResponsePath(), value)
    return writeTagValue(getMissionTriggerLastResponsePath(), value)
