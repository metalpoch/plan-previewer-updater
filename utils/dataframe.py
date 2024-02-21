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


def __df_by_client_status(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_cut_off = df[["DSLAMIP", "Status", "Cantidad"]][df["Status"] == "CORTADO"]
    df_cut_off = df_cut_off.groupby(["DSLAMIP"])["Cantidad"].sum().reset_index()

    df_suspended = df[["DSLAMIP", "Status", "Cantidad"]][df["Status"] == "SUSPENDIDO"]
    df_suspended = df_suspended.groupby(["DSLAMIP"])["Cantidad"].sum().reset_index()

    df_disable = pd.merge(
        df_cut_off, df_suspended, on="DSLAMIP", suffixes=["_cortado", "_suspendido"]
    )

    df_disable.rename(
        inplace=True,
        columns={
            "Cantidad_cortado": "clients_cut_off",
            "Cantidad_suspendido": "clients_suspended",
        },
    )

    df_active = df[df["Status"] == "ACTIVO"]

    df = df_active.groupby(["DSLAMIP", "Downstream"])["Cantidad"].sum().reset_index()

    df_active = df_active[
        ["bras", "Provider.1", "DSLAMIP", "Name Coid"]
    ].drop_duplicates("DSLAMIP")

    return df, df_active, df_disable


def get_aba_data(filename: str) -> pd.DataFrame:
    """
    Reads an Excel file and processes the data to return a DataFrame.

    Parameters:
    filename (str): The name of the Excel file to be read.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """
    df_base = pd.read_excel(
        filename,
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

    df, df_active, df_disable = __df_by_client_status(df_base)

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

    df = df.merge(df_active, on="DSLAMIP")
    df = df.merge(df_disable, on="DSLAMIP")

    return df.rename(
        columns={
            "DSLAMIP": "ip",
            "Provider.1": "model",
            "Name Coid": "central",
            **PLANS_COLUMNS,
        }
    )
