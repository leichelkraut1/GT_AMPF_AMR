from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagPaths import getContainerLocationsPath
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagPaths import getFleetRobotsPath

from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.Paths import plcPlaceRowPath
from MainController.State.Paths import plcRobotRowPath


CONTAINER_LOCATION_HEADERS = ["FleetLocationTagName", "Type"]
DEFAULT_CONTAINER_TEMPLATE_PATH = "[Otto_FleetManager]Fleet/Containers/Templates/Container1"


def _datasetRows(path, headers):
    """Read one dataset tag and return normalized rows keyed by the requested headers."""
    result = readTagValues([path])[0]
    if not result.quality.isGood() or not hasattr(result.value, "getRowCount"):
        return []

    dataset = result.value
    rows = []
    for rowIndex in range(dataset.getRowCount()):
        row = {}
        for header in list(headers or []):
            row[header] = normalizeTagValue(dataset.getValueAt(rowIndex, header))
        rows.append(row)
    return rows


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
    return _datasetRows(getContainerLocationsPath(), CONTAINER_LOCATION_HEADERS)


def _loadContainerLocationConfig():
    """Load the configured create subset and resolve it through the current PLC mappings once."""
    mappingState = dict(readPlcMappings() or {})
    robotMapping = dict(mappingState.get("robot_name_to_plc_tag") or {})
    placeMapping = dict(mappingState.get("place_tag_name_to_plc_tag") or {})
    locations = []

    for row in _configuredLocations():
        fleetLocationTagName = normalizeTagValue(row.get("FleetLocationTagName"))
        locationType = normalizeTagValue(row.get("Type")).lower()
        if not fleetLocationTagName or locationType not in ["place", "robot"]:
            continue

        if locationType == "place":
            plcTagName = normalizeTagValue(placeMapping.get(fleetLocationTagName))
            if not plcTagName:
                continue
            locations.append({
                "type": "place",
                "fleet_location_tag_name": fleetLocationTagName,
                "plc_tag_name": plcTagName,
                "row_path": plcPlaceRowPath(plcTagName),
                "id_path": getFleetPlacesPath() + "/{}/ID".format(fleetLocationTagName),
            })
            continue

        plcTagName = normalizeTagValue(robotMapping.get(fleetLocationTagName))
        if not plcTagName:
            continue
        locations.append({
            "type": "robot",
            "fleet_location_tag_name": fleetLocationTagName,
            "plc_tag_name": plcTagName,
            "row_path": plcRobotRowPath(plcTagName),
            "id_path": getFleetRobotsPath() + "/{}/ID".format(fleetLocationTagName),
        })

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
    import Otto_API.Containers.Get
    import Otto_API.Containers.Post

    logger = system.util.getLogger(str(loggerName or "FleetStatus.ContainerCreate"))
    locationConfig = _loadContainerLocationConfig()
    selectedLocation = resolveSelectedContainerLocation(selectedRowPath, locationConfig=locationConfig)
    if selectedLocation is None:
        logger.warn(
            "Skipping container create because the selected destination is not configured in Fleet/Config/ContainerLocations: {}".format(
                selectedRowPath
            )
        )
        return None

    templatePath = normalizeTagValue(templatePath) or DEFAULT_CONTAINER_TEMPLATE_PATH
    fleetLocationTagName = selectedLocation["fleet_location_tag_name"]
    if selectedLocation["type"] == "place":
        placeId = normalizeTagValue(selectedLocation.get("resolved_id"))
        if not placeId:
            logger.warn(
                "Skipping container create for [{}] because place [{}] no longer resolves".format(
                    selectedRowPath,
                    fleetLocationTagName,
                )
            )
            return None
        result = Otto_API.Containers.Post.createContainerAtPlace(templatePath, placeId)
    elif selectedLocation["type"] == "robot":
        robotId = normalizeTagValue(selectedLocation.get("resolved_id"))
        if not robotId:
            logger.warn(
                "Skipping container create for [{}] because robot [{}] no longer resolves".format(
                    selectedRowPath,
                    fleetLocationTagName,
                )
            )
            return None
        result = Otto_API.Containers.Post.createContainerAtRobot(templatePath, robotId)
    else:
        logger.warn(
            "Skipping container create because the configured type is invalid for {}".format(
                selectedRowPath
            )
        )
        return None

    Otto_API.Containers.Get.updateContainers()
    return result
