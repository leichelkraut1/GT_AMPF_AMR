import json
import time
import uuid

def updateStatusInfo():
    """Legacy helper for manual status refreshes; main loop orchestration lives elsewhere."""
    Otto_API.System.Get.getServerStatus()
