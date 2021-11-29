import pandas as pd


def make_dataframe(**kwargs):
    if kwargs.get("columns"):
        df = pd.DataFrame(kwargs["data"], columns=[kwargs["columns"]])
    else:
        df = pd.DataFrame(kwargs["data"])
    return df


def get_dataframe_from_csv(**kwargs):
    csv = pd.read_csv(kwargs.get("filename"))
    df = pd.DataFrame(csv)
    return df
