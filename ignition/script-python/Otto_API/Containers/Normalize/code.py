import json


def normalizeContainerRecord(containerRecord):
    """
    Normalize an OTTO container record for Fleet/Containers sync.
    """
    containerId = containerRecord.get("id")
    if containerId is None or not str(containerId).strip():
        return None

    return {
        "instance_name": str(containerId).strip(),
        "tag_values": {
            "/ID": containerRecord.get("id"),
            "/ContainerType": containerRecord.get("container_type"),
            "/Created": containerRecord.get("created"),
            "/Description": containerRecord.get("description"),
            "/Empty": containerRecord.get("empty"),
            "/Name": containerRecord.get("name"),
            "/Place": containerRecord.get("place"),
            "/ReservedAt": containerRecord.get("reserved_at"),
            "/ReservedBy": containerRecord.get("reserved_by"),
            "/Robot": containerRecord.get("robot"),
            "/State": containerRecord.get("state"),
            "/SystemCreated": containerRecord.get("system_created"),
            "/jsonString": json.dumps(containerRecord),
        }
    }
