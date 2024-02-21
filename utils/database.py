import re
from os import environ
from itertools import chain

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

from utils.db_tools import (
    extract_data,
    aggregate_creator,
    avg_traffic,
    aggregate_traffic_aba,
)

from constants import columns

PLANS_COLUMNS = columns.PLANS

load_dotenv()

MONGO_URI_TACCESS = environ.get("MONGO_URI_TACCESS", "")
MONGO_URI_PREVIEWER = environ.get("MONGO_URI_PLAN_PREVIEWER", "")


def update_traffic(firstday: str, lastday: str) -> None:
    """
    Updates the traffic data in the database.

    Parameters:
    firstday (str): The first day of the data range.
    lastday (str): The last day of the data range.
    """
    client = MongoClient(MONGO_URI_TACCESS)
    db = client["taccess"]
    data_interfaces = list(db["ipInterfacesEhealth"].find())
    interfaces = []
    states_by_interface = {}
    for interface_and_state in list(map(extract_data, data_interfaces)):
        state = interface_and_state[1]
        interface_list = interface_and_state[0]

        if not states_by_interface.get(state):
            states_by_interface[state] = []

        states_by_interface[state].extend(interface_list)

        interfaces.append(interface_list)

    interfaces = list(chain.from_iterable(interfaces))

    pipeline_aggregate = aggregate_creator(firstday, lastday, interfaces)

    data = (
        list(db.ehealth1.aggregate(pipeline=pipeline_aggregate))
        + list(db.ehealth2.aggregate(pipeline=pipeline_aggregate))
        + list(db.ehealth3.aggregate(pipeline=pipeline_aggregate))
        + list(db.ehealth4.aggregate(pipeline=pipeline_aggregate))
    )

    client.close()

    def __get_summary(element: dict) -> dict | None:
        """
        Returns a summary of the data for an element.

        Parameters:
        element (dict): The element to summarize.

        Returns:
        dict: The summary of the element.
        """
        interface, group = element.get("interface"), element.get("group")
        _in, out, bw = (
            element.get("in", []),
            element.get("out", []),
            element.get("bandwidth", []),
        )
        ip = list(
            filter(
                lambda x: interface in x.get("interfaces", [])
                or interface == x.get("lag", ""),
                data_interfaces,
            )
        )[0]["ip"]

        if interface in interfaces_cached:
            return

        # Get state:
        state = None
        for key, value in states_by_interface.items():
            if interface in value:
                state = key
                break

        interfaces_cached.append(interface)

        return {
            "ip": ip,
            "interface": interface,
            "state": state,
            "group": group,
            "in_avg": avg_traffic(_in),
            "out_avg": avg_traffic(out),
            "bandwidth": max(max(bw)),
        }

    interfaces_cached = []
    docs = list(filter(lambda x: x != None, map(__get_summary, data)))

    client = MongoClient(MONGO_URI_PREVIEWER)
    db = client["planPreviewer"]
    db.traffic.delete_many({})
    db.traffic.insert_many(docs)
    client.close()


def update_aba(df: pd.DataFrame) -> None:
    """
    Updates the ABA data in the database.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the ABA data.
    """
    docs = df.to_dict(orient="records")
    client = MongoClient(MONGO_URI_PREVIEWER)
    db = client["planPreviewer"]
    db.aba.delete_many({})
    db.aba.insert_many(docs)
    client.close()


def __lag_scanner(interface) -> bool:
    """
    Checks if the interface is a LAG interface.

    Parameters:
    interface (str): The interface to check.

    Returns:
    bool: True if the interface is a LAG interface, False otherwise.
    """
    for lag in ["-LAG-", "_LAG_", "-LAG_", "_LAG-"]:
        if lag in interface.upper():
            return True
    return False


def __traffic_scanner(data: dict) -> dict:
    """
    Scans the traffic data and returns a dictionary with the results.

    Parameters:
    data (dict): The traffic data to scan.

    Returns:
    dict: The results of the scan.
    """
    in_avg, out_avg, bw = 0, 0, 0
    in_avg_lag, out_avg_lag, bw_lag = 0, 0, 0
    interface, interface_lag, state = "", "", ""

    for traffic in data["traffic"]:
        is_lag = __lag_scanner(traffic["interface"])
        if is_lag:
            in_avg_lag += traffic["in_avg"]
            out_avg_lag += traffic["out_avg"]
            bw_lag += traffic["bandwidth"]
            interface_lag = traffic["interface"]
        else:
            in_avg += traffic["in_avg"]
            out_avg += traffic["out_avg"]
            bw += traffic["bandwidth"]
            interface = traffic["interface"]
        state = traffic["state"]

    max_bits_interfaces = max(in_avg, out_avg)
    max_bits_lag = max(in_avg_lag, out_avg_lag)
    result = {
        "central": data["central"],
        "ip": data["ip"],
        "theoretical_traffic_mbps": data["theoretical_traffic"],
        "model": data["model"],
        "clients": data["clients"],
        "clients_active": data["clients_active"],
        "clients_cut_off": data["clients_cut_off"],
        "clients_suspended": data["clients_suspended"],
        "state": state,
    }

    for clients_by_plan in PLANS_COLUMNS.values():
        result[clients_by_plan] = data[clients_by_plan]

    if max_bits_lag > max_bits_interfaces:
        match_element = re.search(
            r"^([A-Z0-9-]+)-.*$", interface_lag.upper().split("LAG")[0]
        )
        result["element"] = match_element.group(1) if match_element else ""
        result["in_avg_mbps"] = in_avg_lag / 10**6
        result["out_avg_mbps"] = out_avg_lag / 10**6
        result["bandwidth_mbps"] = bw_lag / 10**6
    else:
        match_element = re.search(r"^([A-Z0-9-]+)-.*$", interface.upper())
        result["element"] = match_element.group(1) if match_element else ""
        result["in_avg_mbps"] = in_avg / 10**6
        result["out_avg_mbps"] = out_avg / 10**6
        result["bandwidth_mbps"] = bw / 10**6

    max_traffic = max(result["in_avg_mbps"], result["out_avg_mbps"])
    result["media"] = max_traffic / result["clients_active"]
    result["factor"] = max_traffic / result["theoretical_traffic_mbps"]

    for plan in PLANS_COLUMNS.values():
        plan_mbps = __str_plan_to_float(plan)
        clients = result[plan]
        result[f"factor_{plan.replace('clients_', '')}"] = (
            clients * plan_mbps * result["factor"]
        )

    return result


def __str_plan_to_float(string: str) -> float:
    """
    Converts a plan from string format to float format.

    Parameters:
    string (str): The plan in string format.

    Returns:
    float: The plan in float format.
    """
    idx = list(PLANS_COLUMNS.values()).index(string)
    plans = list(PLANS_COLUMNS.keys())
    return plans[idx]


def update_traffic_aba() -> None:
    """
    Updates the ABA traffic data in the database.
    """
    client = MongoClient(MONGO_URI_PREVIEWER)
    db = client["planPreviewer"]
    pipeline_aggregate = aggregate_traffic_aba()
    data = list(db.aba.aggregate(pipeline_aggregate))

    docs = list(map(__traffic_scanner, data))
    db.traffic_aba.delete_many({})
    db.traffic_aba.insert_many(docs)
    client.close()
