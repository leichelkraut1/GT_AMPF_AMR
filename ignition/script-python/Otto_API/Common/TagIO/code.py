try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)

from Otto_API.Common.TagPaths import getApiBaseUrlPath
from Otto_API.Common.TagPaths import getMainCycleEndpointsPath


def _normalizeTagLabel(label, tagPath):
    if label:
        return str(label)
    return "Tag"


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


def readTagValues(tagPaths):
    """Read multiple tag values synchronously."""
    tagPaths = list(tagPaths or [])
    if not tagPaths:
        return []
    return system.tag.readBlocking(tagPaths)


def readRequiredTagValue(tagPath, label=None, allowEmptyString=False):
    """Read a required tag value and raise a clear ValueError if it is missing."""
    tagResult = readTagValues([tagPath])[0]
    return _validateTagRead(tagResult, tagPath, label, allowEmptyString)


def readRequiredTagValues(tagPaths, labels=None, allowEmptyString=False):
    """Read multiple required tag values and raise clear ValueErrors for bad entries."""
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
    """Read multiple optional tag values and substitute defaults for bad entries."""
    tagPaths = list(tagPaths or [])
    results = readTagValues(tagPaths)

    if defaultValues is None:
        defaultValues = []
    elif isinstance(defaultValues, (list, tuple)):
        defaultValues = list(defaultValues)
    else:
        defaultValues = [defaultValues] * len(tagPaths)

    values = []
    for index, _tagPath in enumerate(tagPaths):
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
    """Read an optional tag value and return the supplied default when missing."""
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


def normalizeTagValue(value):
    """Normalize a tag value into a trimmed string for comparisons and joins."""
    return str(value or "").strip()


def browseTagResults(path):
    """Browse a tag path and return its result rows."""
    return system.tag.browse(path).getResults()


def browseResultValue(browseResult, key, defaultValue=None):
    """Read one value from an Ignition browse row or local test browse row."""
    if browseResult is None:
        return defaultValue

    if isinstance(browseResult, dict):
        return browseResult.get(key, defaultValue)

    getter = getattr(browseResult, "get", None)
    if getter is not None:
        try:
            return getter(key)
        except TypeError:
            try:
                return getter(key, defaultValue)
            except Exception:
                return defaultValue
        except Exception:
            return defaultValue

    return getattr(browseResult, key, defaultValue)


def browseUdtInstancePaths(path):
    """Return full paths for UDT instances directly below a browsed tag path."""
    instancePaths = []
    for browseResult in list(browseTagResults(path) or []):
        instancePath = str(browseResultValue(browseResult, "fullPath", "") or "")
        tagType = str(browseResultValue(browseResult, "tagType", "") or "").lower()
        if instancePath and "udtinstance" in tagType:
            instancePaths.append(instancePath)
    return instancePaths


def tagExists(path):
    """Return True when the given tag path exists."""
    return system.tag.exists(path)


def deleteTagPaths(tagPaths):
    """Delete one or more tag paths."""
    return system.tag.deleteTags(list(tagPaths))


def deleteTagPath(tagPath):
    """Delete a single tag path."""
    return deleteTagPaths([tagPath])


def isWriteResultGood(result):
    """Return True when a write result represents a successful write."""
    if hasattr(result, "isGood"):
        return bool(result.isGood())

    if isinstance(result, dict):
        quality = result.get("quality")
        if quality is not None and hasattr(quality, "isGood"):
            return bool(quality.isGood())
        return str(quality).strip().lower() == "good"

    try:
        return int(result) == 0
    except Exception:
        return bool(result)


def _buildWriteFailureMessage(tagPath, result, label=None):
    subject = str(label) if label else "Tag write"
    return "{} failed for {}: {}".format(subject, tagPath, result)


def writeTagValue(tagPath, value):
    """Write a single tag value synchronously."""
    return system.tag.writeBlocking([tagPath], [value])


def writeTagValues(tagPaths, values):
    """Write multiple tag values synchronously."""
    return system.tag.writeBlocking(list(tagPaths), list(values))


def writeRequiredTagValue(tagPath, value, label=None):
    """Write a required tag value and raise when the write result is not good."""
    results = writeTagValue(tagPath, value)
    result = results[0] if results else None
    if not isWriteResultGood(result):
        raise ValueError(_buildWriteFailureMessage(tagPath, result, label))
    return results


def writeRequiredTagValues(tagPaths, values, labels=None):
    """Write required tag values and raise when any write result is not good."""
    tagPaths = list(tagPaths or [])
    values = list(values or [])
    labels = list(labels or [])
    results = writeTagValues(tagPaths, values)

    for index, tagPath in enumerate(tagPaths):
        result = results[index] if index < len(results) else None
        label = labels[index] if index < len(labels) else None
        if not isWriteResultGood(result):
            raise ValueError(_buildWriteFailureMessage(tagPath, result, label))

    return results


def writeObservedTagValue(tagPath, value, label=None, logger=None):
    """Write one tag value and warn when the result is not good."""
    results = writeTagValue(tagPath, value)
    result = results[0] if results else None
    if not isWriteResultGood(result):
        if logger is None:
            logger = system.util.getLogger("Otto_API.Common.TagIO")
        logger.warn(_buildWriteFailureMessage(tagPath, result, label))
    return results


def writeObservedTagValues(tagPaths, values, labels=None, logger=None):
    """Write tag values and warn when any result is not good."""
    tagPaths = list(tagPaths or [])
    values = list(values or [])
    labels = list(labels or [])
    results = writeTagValues(tagPaths, values)

    if logger is None:
        logger = system.util.getLogger("Otto_API.Common.TagIO")

    for index, tagPath in enumerate(tagPaths):
        result = results[index] if index < len(results) else None
        label = labels[index] if index < len(labels) else None
        if not isWriteResultGood(result):
            logger.warn(_buildWriteFailureMessage(tagPath, result, label))

    return results


def writeTagValueAsync(tagPath, value):
    """Write a single tag value asynchronously."""
    return system.tag.writeAsync(tagPath, value)


def writeTagValuesAsync(tagPaths, values):
    """Write multiple tag values asynchronously."""
    return system.tag.writeAsync(list(tagPaths), list(values))


def getApiBaseUrl():
    """Read the OTTO API base URL from the shared project tag."""
    return readRequiredTagValue(getApiBaseUrlPath(), "API base URL")


def getOttoOperationsUrl():
    """Build the OTTO operations endpoint from the shared base URL."""
    return getApiBaseUrl().rstrip("/") + "/operations/"
