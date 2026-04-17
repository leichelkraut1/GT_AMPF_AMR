import json
import time
import uuid

def updateStatusInfo():
	#Get status info for the server, missions, and vehicles
	Otto_API.Get.getServerStatus()