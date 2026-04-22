try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit

from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import getDisableLogOfMainCycleHttpPath
from Otto_API.Common.TagHelpers import getFleetConfigPath
from Otto_API.Common.TagHelpers import getFleetRootPath
from Otto_API.Common.TagHelpers import getMainCycleEndpointsPath


MAIN_CYCLE_ENDPOINT_HEADERS = ["Method", "Path"]
DEFAULT_MAIN_CYCLE_ENDPOINT_ROWS = [
    ["GET", "/system/state/"],
    ["GET", "/robots/states/"],
    ["GET", "/robots/activities/"],
    ["GET", "/robots/batteries/"],
    ["GET", "/containers/"],
    ["GET", "/missions/"],
]


def normalizedEndpointKey(method, url):
    method = str(method or "").strip().upper()
    path = str(urlsplit(str(url or "")).path or "/").strip() or "/"
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
        path = str(rawValue.getValueAt(index, "Path") or "").strip()
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
