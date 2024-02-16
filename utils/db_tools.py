def aggregate_creator(firstday: str, lastday: str, interfaces: list[str]) -> list[dict]:
    """
    Creates an aggregation pipeline for MongoDB.

    Parameters:
    firstday (str): The first day of the data range.
    lastday (str): The last day of the data range.
    interfaces (list[str]): The list of interfaces.

    Returns:
    list[dict]: The aggregation pipeline.
    """
    return [
        {
            "$match": {
                "_id": {"$gte": firstday, "$lte": lastday},
            },
        },
        {"$unwind": "$trends"},
        {"$unwind": "$trends.elements"},
        {
            "$match": {
                "trends.elements.interface": {
                    "$in": interfaces,
                },
            },
        },
        {
            "$group": {
                "_id": {
                    "interface": "$trends.elements.interface",
                    "group": "$trends.group",
                },
                "in": {"$push": "$trends.elements.in"},
                "out": {"$push": "$trends.elements.out"},
                "bandwidth": {"$push": "$trends.elements.bandwidth"},
            },
        },
        {
            "$project": {
                "_id": 0,
                "interface": "$_id.interface",
                "group": "$_id.group",
                "in": 1,
                "out": 1,
                "bandwidth": 1,
            },
        },
    ]


def extract_data(data: dict) -> tuple:
    """
    Extracts the interfaces from the data.

    Parameters:
    data (dict): The data to extract interfaces from.

    Returns:
    tuple: The list of interfaces and string of Venezuela state.
    """
    interfaces, lag, state = data.get("interfaces"), data.get("lag"), data.get("state")
    if interfaces is None:
        return [], state

    if lag:
        interfaces.append(lag)
    return interfaces, state


def avg_traffic(traffic: list[list[float]]) -> float:
    """
    Calculates the average traffic.

    Parameters:
    traffic (list[list[float]]): The traffic data.

    Returns:
    float: The average traffic.
    """
    max_traffic_by_day = list(filter(lambda x: x > 0, map(lambda x: max(x), traffic)))

    try:
        return sum(max_traffic_by_day) / len(max_traffic_by_day)
    except ZeroDivisionError:
        return 0


def aggregate_traffic_aba() -> list[dict]:
    """
    Creates an aggregation pipeline for ABA traffic in MongoDB.

    Returns:
    list[dict]: The aggregation pipeline.
    """
    return [
        {
            "$lookup": {
                "from": "traffic",
                "localField": "ip",
                "foreignField": "ip",
                "as": "traffic",
            },
        },
        {
            "$match": {
                "traffic": {"$ne": []},
            },
        },
    ]
