import pandas as pd
from constants import columns


PLANS_COLUMNS = columns.PLANS


def __string_to_ip(string: str) -> str:
    """
    Converts a string to an IP address format.

    Parameters:
    string (str): The string to be converted.

    Returns:
    str: The converted string in IP address format.
    """
    string = str(string)
    if "." in string:
        return string

    string = string[::-1]
    string = ".".join([string[i : i + 3] for i in range(0, len(string), 3)])

    return string[::-1]


def __df_by_client_status(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # clients active
    df_active = (
        df[["DSLAMIP", "Status", "Cantidad"]][df["Status"] == "ACTIVO"]
        .rename(columns={"Cantidad": "clients_active"})
        .groupby(["DSLAMIP"])["clients_active"]
        .sum()
        .reset_index()
    )

    # clients cut off
    df_cut_off = (
        df[["DSLAMIP", "Status", "Cantidad"]][df["Status"] == "CORTADO"]
        .rename(columns={"Cantidad": "clients_cut_off"})
        .groupby(["DSLAMIP"])["clients_cut_off"]
        .sum()
        .reset_index()
    )

    # clients suspend
    df_suspended = (
        df[["DSLAMIP", "Status", "Cantidad"]][df["Status"] == "SUSPENDIDO"]
        .rename(columns={"Cantidad": "clients_suspended"})
        .groupby(["DSLAMIP"])["clients_suspended"]
        .sum()
        .reset_index()
    )

    # status clients
    df_status_clients = pd.merge(df_cut_off, df_suspended, on="DSLAMIP", how="outer")
    df_status_clients = df_status_clients.merge(df_active, on="DSLAMIP", how="outer")

    df = df.groupby(["DSLAMIP", "Downstream"])["Cantidad"].sum().reset_index()

    return df, df_status_clients


def get_aba_data(filename_base: str, filename_total: str) -> pd.DataFrame:
    """
    Reads an Excel file and processes the data to return a DataFrame.

    Parameters:
    filename (str): The name of the Excel file to be read.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """
    try:
        df_total = pd.read_excel(
            filename_total, usecols=["DSLAMIP", "PUERTOS_CON_CONTRATO"]
        )

        df_base = pd.read_excel(
            filename_base,
            usecols=[
                "Coid",
                "Name Coid",
                "Provider.1",
                "DSLAMIP",
                "Nrpname",
                "Location",
                "Downstream",
                "Status",
                "Cantidad",
            ],
        )
    except BaseException:
        exit(
            "Error: los reportes enviados son invalidos, el orden es 'clientes' y luego 'total'"
        )

    df_total.rename(
        inplace=True,
        columns={"DSLAMIP": "ip", "PUERTOS_CON_CONTRATO": "port_with_contract"},
    )

    df_base["bras"] = df_base["Location"] + "-" + df_base["Nrpname"]
    df_base = df_base[
        [
            "bras",
            "Provider.1",
            "DSLAMIP",
            "Coid",
            "Name Coid",
            "Downstream",
            "Status",
            "Cantidad",
        ]
    ]
    df_base["DSLAMIP"] = df_base["DSLAMIP"].apply(__string_to_ip)

    df, df_status_clients = __df_by_client_status(df_base)

    df_base = df_base[["bras", "Provider.1", "DSLAMIP", "Name Coid"]].drop_duplicates(
        "DSLAMIP"
    )

    df["Downstream"] = df["Downstream"] / 1024
    df["theoretical_traffic"] = df["Downstream"] * df["Cantidad"]
    df = (
        df.pivot_table(
            values="Cantidad",
            index=["DSLAMIP", "theoretical_traffic"],
            columns="Downstream",
        )
        .fillna(0)
        .reset_index()
    )
    df = df.groupby(["DSLAMIP"]).sum().reset_index()
    df["clients"] = df.iloc[:, 2:].sum(axis=1)

    df = df.merge(df_base, on="DSLAMIP")
    df = df.merge(df_status_clients, on="DSLAMIP")

    df.rename(
        inplace=True,
        columns={
            "DSLAMIP": "ip",
            "Provider.1": "model",
            "Name Coid": "central",
            **PLANS_COLUMNS,
        },
    )

    return df.merge(df_total, on="ip").fillna(0)
