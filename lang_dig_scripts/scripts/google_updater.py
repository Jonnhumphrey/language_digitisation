import datetime
import pygsheets


def init_google_sheet():
    gc = pygsheets.authorize(service_file='/Users/jonh/lang_dig_scripts/json_files/'
                                          'language-digitization-project-5c87586d60ad.json')
    return gc

def open_google_sheet(gc, sheetname):
    sheet = gc.open(sheetname)
    return sheet

def set_google_sheet(sheet, sheet_name, df):
    print("Setting sheet for : ", sheet_name)
    wks = sheet.worksheet_by_title(sheet_name)
    wks.clear()
    wks.set_dataframe(df, (1, 1))
    print("Sheet set for : ", sheet_name)

def update_cell(sheet, sheet_name):
    wks = sheet.worksheet_by_title(sheet_name)
    wks.update_value('B1', datetime.datetime.now().strftime("%Y/%m/%d, %H:%M:%S"))
    print("Timestamp updated")

def update_google_sheets(sheet, list_of_dfs):
    for site in list_of_dfs:
        set_google_sheet(sheet, list_of_dfs[site].get("filename"), list_of_dfs[site].get("df"))
    return list_of_dfs