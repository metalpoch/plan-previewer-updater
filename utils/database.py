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
    interfaces = list(map(extract_data, data_interfaces))
    interfaces = list(chain.from_iterable(interfaces))

    pipeline_aggregate = aggregate_creator(firstday, lastday, interfaces)

    data = list(db.ehealth3.aggregate(pipeline=pipeline_aggregate)) + list(
        db.ehealth2.aggregate(pipeline=pipeline_aggregate)
    )

    client.close()

    def __get_summary(element: dict) -> dict:
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

        return {
            "ip": ip,
            "interface": interface,
            "group": group,
            "in_avg": avg_traffic(_in),
            "out_avg": avg_traffic(out),
            "bandwidth": avg_traffic(bw),
        }

    docs = list(map(__get_summary, data))

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
    interface, interface_lag = "", ""

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

    max_bits_interfaces = max(in_avg, out_avg)
    max_bits_lag = max(in_avg_lag, out_avg_lag)
    result = {
        "central": data["central"],
        "ip": data["ip"],
        "theoretical_traffic_mbps": data["theoretical_traffic"],
        "clients": data["clients"],
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
    result["media"] = max_traffic / result["clients"]
    result["factor"] = max_traffic / result["theoretical_traffic_mbps"]

    last_plan = list(PLANS_COLUMNS.values())[-1]
    result["upgrade"] = __change_plan(result, last_plan)
    return result


def __change_plan(data: dict, base_plan: str) -> dict:
    """
    Changes the plan based on the data and the base plan.

    Parameters:
    data (dict): The data to use for changing the plan.
    base_plan (str): The base plan to change from.

    Returns:
    dict: The results of the plan change.
    """

    def str_plan_to_float(string: str) -> float:
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

    columns_to_delete = [
        "ip",
        "theoretical_traffic_mbps",
        "clients",
        "element",
        "in_avg_mbps",
        "out_avg_mbps",
        "bandwidth_mbps",
        "media",
        "factor",
    ]

    plans = list(PLANS_COLUMNS.values())
    plan_idx = plans.index(base_plan)
    possible_plans = list(
        map(lambda x: plans[x], filter(lambda idx: idx <= plan_idx, range(len(plans))))
    )[::-1]

    result = {}

    for plan in possible_plans:
        idx = plans.index(plan)
        plans_filter = filter(lambda x: x < idx, range(len(plans)))
        other_plans = list(map(lambda x: plans[x], plans_filter))

        tmp = data.copy()
        tmp["benefited_clients"] = 0
        tmp["new_theoretical_traffic_mbps"] = 0
        for old_plan in other_plans:
            tmp["benefited_clients"] += data[old_plan]
            tmp[plan] += data[old_plan]
            tmp[old_plan] = 0

        for field in plans:
            float_plan = str_plan_to_float(field)
            new_traffic_plans = tmp[field] * float_plan * tmp["factor"]
            tmp[f"factor_{field}"] = new_traffic_plans
            tmp["new_theoretical_traffic_mbps"] += new_traffic_plans

        bw = tmp["bandwidth_mbps"]
        is_upgradable = bw * 0.8 >= tmp["new_theoretical_traffic_mbps"]  # 20%
        tmp["is_upgradable"] = True if is_upgradable else False

        for key in columns_to_delete:
            del tmp[key]

        result[f"{plan.replace('clients_', '')}"] = tmp

    return result


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
