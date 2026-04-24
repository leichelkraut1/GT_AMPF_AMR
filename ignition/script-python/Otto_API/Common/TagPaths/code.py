PROJECT_ROOT_TAG_PATH = "[Otto_FleetManager]"
FLEET_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "Fleet"
FLEET_SYSTEM_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/System"
FLEET_ROBOTS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Robots"
FLEET_MISSIONS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Missions"
FLEET_CONFIG_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Config"
FLEET_TRIGGERS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Triggers"
FLEET_PLACES_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Places"
FLEET_MAPS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Maps"
FLEET_WORKFLOWS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Workflows"
FLEET_CONTAINERS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Containers"
FLEET_INTERLOCKS_ROOT_TAG_PATH = FLEET_ROOT_TAG_PATH + "/Interlocks"
FLEET_CONTAINERS_VERBOSE_CLEANUP_LOGGING_TAG_PATH = FLEET_CONTAINERS_ROOT_TAG_PATH + "/VerboseCleanupLogging"
PLC_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "PLC"
PLC_INTERLOCKS_ROOT_TAG_PATH = PLC_ROOT_TAG_PATH + "/Interlocks"
MAINCONTROL_ROOT_TAG_PATH = PROJECT_ROOT_TAG_PATH + "MainControl"
MAINCONTROL_INTERNAL_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Internal"
MAINCONTROL_ROBOTS_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Robots"
MAINCONTROL_RUNTIME_ROOT_TAG_PATH = MAINCONTROL_ROOT_TAG_PATH + "/Runtime"

API_BASE_URL_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/Url_ApiBase"
SYSTEM_LAST_RESPONSE_TAG_PATH = FLEET_SYSTEM_ROOT_TAG_PATH + "/lastResponse"
MISSION_TRIGGER_LAST_RESPONSE_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/Triggers/lastResponse"
MISSION_MIN_CHARGE_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/MinChargeLevelForMissioning"
MISSION_MAX_COMPLETED_COUNT_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/MaxCompletedCount"
ROBOT_CHARGING_DELAY_MS_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/ChargingDelayMs"
PENDING_CREATE_MISSION_TIMEOUT_MS_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/PendingCreateMissionTimeoutMs"
DISABLE_MAIN_CYCLE_HTTP_LOGGING_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/DisableLogOfMainCycleHTTP"
MAIN_CYCLE_ENDPOINTS_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/MainCycleEndpoints"
WORKFLOW_CONFIG_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/WorkflowConfig"
CONTAINER_LOCATIONS_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/ContainerLocations"
INTERLOCK_PLC_MAPPING_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/InterlockPlcMapping"
INTERLOCK_WRITEBACK_RETRY_MS_TAG_PATH = FLEET_CONFIG_ROOT_TAG_PATH + "/InterlockWritebackRetryMs"
MISSION_LAST_UPDATE_TS_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/LastUpdateTS"
MISSION_LAST_UPDATE_SUCCESS_TAG_PATH = FLEET_MISSIONS_ROOT_TAG_PATH + "/LastUpdateSuccess"


def getProjectRootPath():
    return PROJECT_ROOT_TAG_PATH


def getFleetRootPath():
    return FLEET_ROOT_TAG_PATH


def getFleetSystemPath():
    return FLEET_SYSTEM_ROOT_TAG_PATH


def getFleetConfigPath():
    return FLEET_CONFIG_ROOT_TAG_PATH


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


def getFleetContainersPath():
    return FLEET_CONTAINERS_ROOT_TAG_PATH


def getFleetInterlocksPath():
    return FLEET_INTERLOCKS_ROOT_TAG_PATH


def getFleetContainersVerboseCleanupLoggingPath():
    return FLEET_CONTAINERS_VERBOSE_CLEANUP_LOGGING_TAG_PATH


def getPlcRootPath():
    return PLC_ROOT_TAG_PATH


def getPlcInterlocksPath():
    return PLC_INTERLOCKS_ROOT_TAG_PATH


def getMainControlRootPath():
    return MAINCONTROL_ROOT_TAG_PATH


def getMainControlInternalPath():
    return MAINCONTROL_INTERNAL_ROOT_TAG_PATH


def getMainControlRobotsPath():
    return MAINCONTROL_ROBOTS_ROOT_TAG_PATH


def getMainControlRuntimePath():
    return MAINCONTROL_RUNTIME_ROOT_TAG_PATH


def getApiBaseUrlPath():
    return API_BASE_URL_TAG_PATH


def getMissionMinChargePath():
    return MISSION_MIN_CHARGE_TAG_PATH


def getMissionMaxCompletedCountPath():
    return MISSION_MAX_COMPLETED_COUNT_TAG_PATH


def getRobotChargingDelayMsPath():
    return ROBOT_CHARGING_DELAY_MS_TAG_PATH


def getPendingCreateMissionTimeoutMsPath():
    return PENDING_CREATE_MISSION_TIMEOUT_MS_TAG_PATH


def getDisableLogOfMainCycleHttpPath():
    return DISABLE_MAIN_CYCLE_HTTP_LOGGING_TAG_PATH


def getMainCycleEndpointsPath():
    return MAIN_CYCLE_ENDPOINTS_TAG_PATH


def getWorkflowConfigPath():
    return WORKFLOW_CONFIG_TAG_PATH


def getContainerLocationsPath():
    return CONTAINER_LOCATIONS_TAG_PATH


def getInterlockPlcMappingPath():
    return INTERLOCK_PLC_MAPPING_TAG_PATH


def getInterlockWritebackRetryMsPath():
    return INTERLOCK_WRITEBACK_RETRY_MS_TAG_PATH


def getMissionLastUpdateTsPath():
    return MISSION_LAST_UPDATE_TS_TAG_PATH


def getMissionLastUpdateSuccessPath():
    return MISSION_LAST_UPDATE_SUCCESS_TAG_PATH


def getSystemLastResponsePath():
    return SYSTEM_LAST_RESPONSE_TAG_PATH


def getMissionTriggerLastResponsePath():
    return MISSION_TRIGGER_LAST_RESPONSE_TAG_PATH


def splitTagPath(path):
    """Split a full Ignition tag path into (parentPath, childName)."""
    path = str(path)
    if "/" in path:
        return path.rsplit("/", 1)

    if "]" in path:
        providerPath, childName = path.split("]", 1)
        return providerPath + "]", childName

    raise ValueError("Unsupported tag path: {}".format(path))
