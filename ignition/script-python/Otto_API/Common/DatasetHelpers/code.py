def datasetWithHeaders(headers, rows=None):
    return system.dataset.toDataSet(list(headers or []), list(rows or []))


def datasetHeaders(datasetValue):
    if datasetValue is None or not hasattr(datasetValue, "getColumnCount"):
        return None

    if hasattr(datasetValue, "getColumnNames"):
        return [
            str(header or "")
            for header in list(datasetValue.getColumnNames() or [])
        ]

    headers = []
    for columnIndex in range(datasetValue.getColumnCount()):
        headers.append(str(datasetValue.getColumnName(columnIndex) or ""))
    return headers


def datasetRows(datasetValue, expectedHeaders, normalizeFunc=None):
    actualHeaders = datasetHeaders(datasetValue)
    if actualHeaders is None:
        return None, "value is not a dataset"

    expectedHeaders = list(expectedHeaders or [])
    if list(actualHeaders) != expectedHeaders:
        return None, "expected headers [{}], found [{}]".format(
            ", ".join(expectedHeaders),
            ", ".join(actualHeaders),
        )

    rows = []
    for rowIndex in range(datasetValue.getRowCount()):
        row = {}
        for header in expectedHeaders:
            value = datasetValue.getValueAt(rowIndex, header)
            if normalizeFunc is not None:
                value = normalizeFunc(value)
            row[header] = value
        rows.append(row)
    return rows, ""
