try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)


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


def readRequiredTagValue(tagPath, label=None, allowEmptyString=False):
    """
    Read a required tag value and raise a clear ValueError if it is missing.
    """
    tagResult = system.tag.readBlocking([tagPath])[0]
    return _validateTagRead(tagResult, tagPath, label, allowEmptyString)


def readOptionalTagValue(tagPath, defaultValue=None, allowEmptyString=False):
    """
    Read an optional tag value and return the supplied default when missing.
    """
    tagResult = system.tag.readBlocking([tagPath])[0]
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
