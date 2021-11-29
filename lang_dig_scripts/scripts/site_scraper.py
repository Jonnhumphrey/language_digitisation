from bs4 import BeautifulSoup
from collections import defaultdict
from dataframe_utils import make_dataframe, get_dataframe_from_csv
import requests
import json
import pandas as pd



def combine_csv():
    lang_one = get_dataframe_from_csv(filename="/Users/jonh/lang_dig_scripts/files/"
                                               "/Languages_one.csv")
    lang_two = get_dataframe_from_csv(filename="/Users/jonh/lang_dig_scripts/files/"
                                               "/Languages_two.csv")
    merged = pd.merge(lang_one, lang_two, how="outer")
    merged.to_csv("languages_combined.csv", index=False)


def table_scraper(**kwargs):
    table_header_cols = []
    table_data = []
    table_header_rows = []
    table_headers_std = []
    lang_table = get_data(**kwargs)
    if lang_table:
        if lang_table.find_all("th", scope="col"):
            for thc in lang_table.find_all("th", scope="col"):
                table_header_cols.append(thc.text)
        elif lang_table.find_all("th", scope="row"):
            for thr in lang_table.find_all("th", scope="row"):
                table_header_rows.append(thr.text)
        elif lang_table.find_all("th"):
            for th in lang_table.find_all("th"):
                table_headers_std.append(th.text)

        table_rows = lang_table.find_all("tr")

        for tr in table_rows:
            row = [td.text.strip() for td in tr.find_all('td') if td.text.strip()]
            if row:
                table_data.append(row)
    if table_header_rows:
        updated_table = [[header_rows.strip()] + table_rows for header_rows, table_rows in
                         zip(table_header_rows, table_data)]
    else:
        updated_table = table_data
    if table_header_cols:
        table_headers_std = table_header_cols

    table_h = [t_h.strip().replace("\n", "") for t_h in table_headers_std]
    kwargs["data"], kwargs["columns"] = updated_table, table_h,
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs["filename"], index=False)
    print("csv made : ", kwargs["filename"])
    return df


def pandas_table_scraper(**kwargs):
    html_content = requests.get(kwargs.get("url"))
    df_list = pd.read_html(html_content.text)
    df = df_list[kwargs.get("index")].fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs.get("filename"))
    return df


def pypi_scraper(**kwargs):
    code = kwargs.get("soup").find_all("code")[2].getText()
    kwargs["data"] = list(code.replace("\n", "").replace(" ", "").split(','))
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs["filename"])
    return df


def gboard_scraper(**kwargs):
    outer_tag = kwargs.get("soup").find_all("div", class_=kwargs.get("cls"))
    result_data = []
    for inner_tag in outer_tag:
        ul = inner_tag.find_all("ul")
        for li in ul:
            result_data.extend(li.text.replace("\xa0", "").split("\n")[1:-1])
    kwargs["data"] = result_data
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs["filename"])
    return df


def form_scraper(**kwargs):
    form_options = kwargs.get("soup").select("option[value]")
    code = [item.get("value") for item in form_options]
    lang = [item.text.replace("\u200e", "") for item in form_options]
    kwargs["data"] = list(zip(code, lang))
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs["filename"])
    return df


def github_cldr_scraper(**kwargs):
    table_data = get_data(**kwargs)
    codes = []
    if table_data:
        tr = table_data.find_all("tr")
        for data in tr:
            if kwargs.get("start"):
                lines = [td.text.strip() for index in range(kwargs["start"], kwargs["end"]) for td in
                         data.find_all("td", id="LC" + str(index))]
            elif kwargs.get("line"):
                lines = [td.text.strip() for td in data.find_all("td", id="LC" + str(kwargs["line"]))]
            if lines:
                codes.append(lines)
    kwargs["data"] = codes
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs["filename"])
    return df


def hyperlink_scraper(**kwargs):
    # soup, tag, filename, cls, cols
    data = get_data(**kwargs)
    stored_data = []
    for tag in data:
        stored_data.append(tag.text)
    kwargs["data"] = stored_data
    df = make_dataframe(**kwargs).fillna("")
    df.to_csv(kwargs.get("filename"), index=False)
    print("csv made : ", kwargs["filename"])
    return df


def scrape_sites():
    with open('/Users/jonh/lang_dig_scripts/json_files/sites_to_scrape.json') as json_file:
        site_details = json.load(json_file)
    site_results = defaultdict(dict)
    print("Scraping sites...")
    for site_key, site_info in site_details.items():
        site_results[site_key]["df"] = \
            globals()[site_info["scrape_func"]] \
                (soup=get_soup(site_info.get("url")),
                 tag=site_info.get("tag"),
                 filename=site_info.get("filename"),
                 index=site_info.get("index"),
                 cls=site_info.get("cls"),
                 identifier=site_info.get("identifier"),
                 columns=site_info.get("columns"),
                 start=site_info.get("start"),
                 end=site_info.get("end"),
                 url=site_info.get("url"))
        site_results[site_key]["site"] = site_info.get("url")
        site_results[site_key]["filename"] = site_info.get("filename")
        print("Site scraped : ", site_info.get("url"))
    return site_results


def get_data(**kwargs):
    if kwargs.get("tag") == "a":
        data = kwargs["soup"].find_all(kwargs["tag"], class_=kwargs.get("cls"))
    elif not kwargs.get("index") and kwargs.get("cls"):
        data = kwargs["soup"].find(kwargs["tag"], class_=kwargs["cls"])
    elif not kwargs.get("index") and kwargs.get("identifier"):
        data = kwargs["soup"].find(kwargs["tag"], id=kwargs["identifier"])
    elif not kwargs.get("index") and not kwargs.get("identifier") and not kwargs.get("cls"):
        data = kwargs["soup"].find(kwargs["tag"])
    elif kwargs.get("index"):
        data = kwargs["soup"].find_all("table")[kwargs["index"]]
    return data


def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    if url:
        html_content = requests.get(url, headers=headers).text
        soup = BeautifulSoup(html_content, features="lxml")
    else:
        soup = None
    return soup



