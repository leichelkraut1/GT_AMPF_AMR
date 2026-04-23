try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit

from Otto_API.Common.TagPaths import getDisableLogOfMainCycleHttpPath
from Otto_API.Common.TagPaths import getFleetConfigPath
from Otto_API.Common.TagPaths import getFleetRootPath
from Otto_API.Common.TagPaths import getMainCycleEndpointsPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag


MAIN_CYCLE_ENDPOINT_HEADERS = ["Method", "Path"]
DEFAULT_MAIN_CYCLE_ENDPOINT_ROWS = [
    ["GET", "/system/state/"],
    ["GET", "/robots/states/"],
    ["GET", "/robots/activities/"],
    ["GET", "/robots/batteries/"],
    ["GET", "/robots/places/"],
    ["GET", "/containers/"],
    ["GET", "/missions/"],
]


def _normalizePath(pathOrUrl):
    rawPath = str(urlsplit(str(pathOrUrl or "")).path or "/").strip()
    if not rawPath:
        return "/"

    path = rawPath.replace("\\", "/")
    while "//" in path:
        path = path.replace("//", "/")

    apiPrefix = "/api/fleet/v2"
    loweredPath = path.lower()
    if loweredPath == apiPrefix:
        path = "/"
    elif loweredPath.startswith(apiPrefix + "/"):
        path = path[len(apiPrefix):]

    stripped = path.strip("/")
    if not stripped:
        return "/"
    return "/" + stripped + "/"


def normalizedEndpointKey(method, url):
    method = str(method or "").strip().upper()
    path = _normalizePath(url)
    return "{} {}".format(method, path)


def _isDatasetLike(rawValue):
    return hasattr(rawValue, "getRowCount") and hasattr(rawValue, "getValueAt")


def buildEndpointDataset(rows=None):
    return system.dataset.toDataSet(
        MAIN_CYCLE_ENDPOINT_HEADERS,
        list(rows or DEFAULT_MAIN_CYCLE_ENDPOINT_ROWS),
    )


def parseEndpointRows(rawValue):
    rows = []
    if not _isDatasetLike(rawValue):
        return rows

    for index in range(int(rawValue.getRowCount() or 0)):
        method = str(rawValue.getValueAt(index, "Method") or "").strip().upper()
        path = _normalizePath(rawValue.getValueAt(index, "Path"))
        if not method or not path:
            continue
        rows.append([method, path])
    return rows


def parseEndpointList(rawValue):
    return set(
        "{} {}".format(method, path)
        for method, path in parseEndpointRows(rawValue)
    )

def ensureHttpLogConfigTags():
    ensureFolder(getFleetRootPath())
    ensureFolder(getFleetConfigPath())
    ensureMemoryTag(getDisableLogOfMainCycleHttpPath(), "Boolean", False)
    ensureMemoryTag(getMainCycleEndpointsPath(), "DataSet", buildEndpointDataset())
