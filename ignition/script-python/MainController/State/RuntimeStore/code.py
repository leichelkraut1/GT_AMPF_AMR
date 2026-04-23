from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import writeRequiredTagValues

from MainController.State.Coerce import toBool
from MainController.State.Paths import runtimePaths


def readRuntimeState():
    """Read only the runtime fields needed for overlap protection."""
    paths = runtimePaths()
    values = readTagValues([
        paths["loop_is_running"],
        paths["loop_overlap_count"],
    ])
    return {
        "loop_is_running": toBool(values[0].value if values[0].quality.isGood() else False),
        "loop_overlap_count": int(values[1].value or 0) if values[1].quality.isGood() else 0,
    }


def writeRuntimeFields(fieldValues):
    """Write a partial set of runtime telemetry fields by logical name."""
    paths = runtimePaths()
    writePaths = []
    writeValues = []
    for fieldName, value in list(dict(fieldValues or {}).items()):
        path = paths.get(fieldName)
        if not path:
            continue
        writePaths.append(path)
        writeValues.append(value)
    if writePaths:
        writeRequiredTagValues(
            writePaths,
            writeValues,
            labels=["MainController runtime state"] * len(writePaths)
        )
