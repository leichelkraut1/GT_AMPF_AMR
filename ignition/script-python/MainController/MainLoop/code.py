from MainController import CommandCalls


PILOT_CREATE_WF1_RV1 = {
    "name": "Create_WF1_RV1",
    "command_group_path": "[Otto_FleetManager]Commands/Missions",
    "command_path": "[Otto_FleetManager]Commands/Missions/Create_WF1_RV1",
    "command_type": "create_mission",
    "template_tag_path": "[Otto_FleetManager]Workflows/WF1_PrimusService/jsonString",
    "robot_id_tag_path": "[Otto_FleetManager]Robots/AMPF_AMR_RV1/ID",
    "robot_name": "AMPF_AMR_RV1",
    "mission_name": "Service Primus with RV1",
    "retry_delay_ms": 5000,
    "max_attempts": 3,
}


def runPilotCreateWF1RV1(nowEpochMs=None, uuidFactory=None, executeMission=None):
    return CommandCalls.runConfiguredCommand(
        PILOT_CREATE_WF1_RV1,
        nowEpochMs=nowEpochMs,
        uuidFactory=uuidFactory,
        executeMission=executeMission,
    )
