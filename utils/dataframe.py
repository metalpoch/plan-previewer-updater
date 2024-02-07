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
        ["bras", "DSLAMIP", "Coid", "Name Coid", "Downstream", "Status", "Cantidad"]
    ]
    df_base["DSLAMIP"] = df_base["DSLAMIP"].apply(__string_to_ip)

    df_base = df_base[df_base["Status"] == "ACTIVO"]

    df = df_base.groupby(["DSLAMIP", "Downstream"])["Cantidad"].sum().reset_index()
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

    df_base_2 = df_base[["bras", "DSLAMIP", "Name Coid"]].drop_duplicates("DSLAMIP")
    df = df.merge(df_base_2, on="DSLAMIP")
    return df.rename(columns={"DSLAMIP": "ip", "Name Coid": "central", **PLANS_COLUMNS})
