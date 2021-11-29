
import pandas as pd
from dataframe_utils import get_dataframe_from_csv
from collections import defaultdict
import json


def merge_files_by_column(**kwargs):
    file_to_group_with = get_dataframe_from_csv(
        filename="/Users/jonh/lang_dig_scripts/files/" +
                 kwargs.get("file_to_group_with"))

    file_without_group = get_dataframe_from_csv(filename="/Users/jonh/lang_dig_scripts/files/" +
                                                         kwargs.get("file_without_group"))

    grouped_file = pd.merge(file_without_group, file_to_group_with, how="left",
                            right_on=kwargs.get("column_name_with_group"),
                            left_on=kwargs.get("column_name_without_group")).fillna("")
    grouped_file = grouped_file.drop(
        columns=kwargs.get("columns_to_drop"))
    grouped_file.to_csv(kwargs.get("file_to_csv"), index=False)
    return grouped_file, kwargs.get("file_to_csv")


def group_pages():
    with open('/Users/jonh/lang_dig_scripts/json_files/sites_to_group.json') as json_file:
        sites_to_group = json.load(json_file)
    print("Grouping files...")
    site_results = defaultdict(dict)
    for site_key, site_info in sites_to_group.items():
        site_results[site_key]["df"], \
        site_results[site_key]["filename"] = \
            merge_files_by_column(file_to_group_with=site_info.get("file_to_group_with"),
                                  file_without_group=site_info.get("file_without_group"),
                                  file_to_csv=site_info.get("file_to_csv"),
                                  column_name_without_group=site_info.get("column_name_without_group"),
                                  column_name_with_group=site_info.get("column_name_with_group"),
                                  columns_to_drop=site_info.get("columns_to_drop"))
        site_results[site_key]["site"] = site_info.get("file_to_csv")
        print("Csv made : ", site_info.get("file_to_csv"))
    return site_results
