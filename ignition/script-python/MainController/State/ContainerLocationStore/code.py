from Otto_API.Common.DatasetHelpers import datasetRows
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagPaths import getContainerLocationsPath
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagPaths import getFleetRobotsPath

from MainController.State.PlcMappingStore import PlcMappingState
from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.Paths import plcPlaceRowPath
from MainController.State.Paths import plcRobotRowPath


CONTAINER_LOCATION_HEADERS = ["FleetLocationTagName", "Type"]
DEFAULT_CONTAINER_TEMPLATE_PATH = "[Otto_FleetManager]Fleet/Containers/Templates/Container1"
_LOCATION_TYPE_CONFIG = {
    "place": {
        "mapping_attr": "place_tag_name_to_plc_tag",
        "row_path": plcPlaceRowPath,
        "id_path": lambda fleetLocationTagName: getFleetPlacesPath() + "/{}/ID".format(fleetLocationTagName),
    },
    "robot": {
        "mapping_attr": "robot_name_to_plc_tag",
        "row_path": plcRobotRowPath,
        "id_path": lambda fleetLocationTagName: getFleetRobotsPath() + "/{}/ID".format(fleetLocationTagName),
    },
}


def _readDatasetRows(path, headers):
    """Read one dataset tag and return normalized rows keyed by the requested headers."""
    result = readTagValues([path])[0]
    if not result.quality.isGood() or not hasattr(result.value, "getRowCount"):
        return []

    rows, _errorMessage = datasetRows(result.value, headers, normalizeTagValue)
    return list(rows or [])


def _resolvedLocationIds(locations):
    """Resolve Fleet place/robot IDs for all configured create destinations in one batched read."""
    locations = list(locations or [])
    idPaths = [location["id_path"] for location in locations]
    if not idPaths:
        return {}

    readResults = readTagValues(idPaths)
    resolvedIds = {}
    for path, result in zip(idPaths, list(readResults or [])):
        if result.quality.isGood():
            resolvedIds[path] = normalizeTagValue(result.value)
        else:
            resolvedIds[path] = ""
    return resolvedIds


def _configuredLocations():
    """Read the configured Fleet location subset that should appear in the create dropdown."""
    return _readDatasetRows(getContainerLocationsPath(), CONTAINER_LOCATION_HEADERS)


def _buildLocationConfig(row, mappingState):
    fleetLocationTagName = normalizeTagValue(row.get("FleetLocationTagName"))
    locationType = normalizeTagValue(row.get("Type")).lower()
    typeConfig = _LOCATION_TYPE_CONFIG.get(locationType)
    if not fleetLocationTagName or typeConfig is None:
        return None

    mapping = getattr(mappingState, typeConfig["mapping_attr"])
    plcTagName = normalizeTagValue(mapping.get(fleetLocationTagName))
    if not plcTagName:
        return None

    return {
        "type": locationType,
        "fleet_location_tag_name": fleetLocationTagName,
        "plc_tag_name": plcTagName,
        "row_path": typeConfig["row_path"](plcTagName),
        "id_path": typeConfig["id_path"](fleetLocationTagName),
    }


def _loadContainerLocationConfig():
    """Load the configured create subset and resolve it through the current PLC mappings once."""
    mappingState = readPlcMappings()
    mappingState = PlcMappingState.fromDict(mappingState)
    locations = []

    for row in _configuredLocations():
        location = _buildLocationConfig(row, mappingState)
        if location is not None:
            locations.append(location)

    resolvedIds = _resolvedLocationIds(locations)
    for location in locations:
        location["resolved_id"] = normalizeTagValue(resolvedIds.get(location["id_path"]))
    return locations


def buildContainerCreateOptions(locationConfig=None):
    """Build FleetStatus create options from the configured subset plus resolved mappings."""
    locationConfig = list(locationConfig or _loadContainerLocationConfig() or [])
    options = []

    for location in locationConfig:
        if not normalizeTagValue(location.get("resolved_id")):
            continue
        options.append({
            "label": location["plc_tag_name"],
            "value": location["row_path"],
        })

    return options


def resolveSelectedContainerLocation(selectedRowPath, locationConfig=None):
    """Resolve a selected PLC row path back to the configured Fleet place or robot row."""
    selectedRowPath = normalizeTagValue(selectedRowPath)
    if not selectedRowPath:
        return None

    for location in list(locationConfig or _loadContainerLocationConfig() or []):
        if location["row_path"] == selectedRowPath:
            return location

    return None


def createContainerFromSelectedRowPath(selectedRowPath, templatePath=None, loggerName="FleetStatus.ContainerCreate"):
    """Create a container at the configured Fleet place or robot behind a selected PLC row path."""
    import Otto_API.Services.Containers

    logger = system.util.getLogger(str(loggerName or "FleetStatus.ContainerCreate"))
    locationConfig = _loadContainerLocationConfig()
    selectedLocation = resolveSelectedContainerLocation(selectedRowPath, locationConfig=locationConfig)
    if selectedLocation is None:
        message = (
            "Skipping container create because the selected destination is not configured "
            "in Fleet/Config/ContainerLocations: {}"
        )
        logger.warn(
            message.format(selectedRowPath)
        )
        return None

    templatePath = normalizeTagValue(templatePath) or DEFAULT_CONTAINER_TEMPLATE_PATH
    fleetLocationTagName = selectedLocation["fleet_location_tag_name"]
    createFunctions = {
        "place": Otto_API.Services.Containers.createContainerAtPlace,
        "robot": Otto_API.Services.Containers.createContainerAtRobot,
    }
    createFunction = createFunctions.get(selectedLocation["type"])
    if createFunction is None:
        logger.warn(
            "Skipping container create because the configured type is invalid for {}".format(
                selectedRowPath
            )
        )
        return None

    locationId = normalizeTagValue(selectedLocation.get("resolved_id"))
    if not locationId:
        logger.warn(
            "Skipping container create for [{}] because {} [{}] no longer resolves".format(
                selectedRowPath,
                selectedLocation["type"],
                fleetLocationTagName,
            )
        )
        return None

    result = createFunction(templatePath, locationId)
    Otto_API.Services.Containers.updateContainers()
    return result
