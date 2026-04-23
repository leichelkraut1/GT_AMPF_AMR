from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeTagValue
from Otto_API.Common.TagPaths import getApiBaseUrlPath
from Otto_API.Common.TagPaths import getContainerLocationsPath
from Otto_API.Common.TagPaths import getFleetConfigPath
from Otto_API.Common.TagPaths import getFleetRootPath
from Otto_API.Common.TagPaths import getMissionMaxCompletedCountPath
from Otto_API.Common.TagPaths import getMissionMinChargePath
from Otto_API.Common.TagPaths import getPendingCreateMissionTimeoutMsPath
from Otto_API.Common.TagPaths import getRobotChargingDelayMsPath
from Otto_API.Common.TagPaths import splitTagPath


def configureTagDefinitions(parentPath, tagDefs, collisionPolicy="i"):
    """Configure one or more tag definitions under the given parent path."""
    return system.tag.configure(parentPath, list(tagDefs), collisionPolicy)


def ensureFolder(path):
    """Ensure a tag folder exists at the given full path."""
    parentPath, name = splitTagPath(path)
    return configureTagDefinitions(
        parentPath,
        [{"name": name, "tagType": "Folder"}],
        "i"
    )


def ensureUdtInstance(parentPath, name, typeId, collisionPolicy="i"):
    """Ensure a UDT instance exists under the given parent folder."""
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
    """Ensure a UDT instance exists at the given full path."""
    parentPath, name = splitTagPath(path)
    return ensureUdtInstance(parentPath, name, typeId, collisionPolicy)


def ensureMemoryTag(path, dataType, initialValue=None, collisionPolicy="i"):
    """Ensure a memory-backed atomic tag exists at the given full path."""
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


def ensureBaseFleetConfigTags():
    """Ensure the shared Fleet/Config memory tags exist with core defaults only."""
    ensureFolder(getFleetRootPath())
    ensureFolder(getFleetConfigPath())
    ensureMemoryTag(getApiBaseUrlPath(), "String", "")
    ensureMemoryTag(getRobotChargingDelayMsPath(), "Int8", 0)
    ensureMemoryTag(getPendingCreateMissionTimeoutMsPath(), "Int8", 30000)
    ensureMemoryTag(getMissionMinChargePath(), "Float4", 0.0)
    ensureMemoryTag(getMissionMaxCompletedCountPath(), "Int4", 20)
    ensureMemoryTag(
        getContainerLocationsPath(),
        "DataSet",
        system.dataset.toDataSet(
            ["FleetLocationTagName", "Type"],
            [],
        ),
    )
