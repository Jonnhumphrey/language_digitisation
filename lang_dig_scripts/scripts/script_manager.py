import sys
from site_scraper import scrape_sites
from google_updater import update_google_sheets, update_cell, get_sheet
from file_merger import group_pages
import argparse


def setup_parser():
    parser = argparse.ArgumentParser("Site Scraper and Google Document Processor",
                                     description="Specify argument for processing",
                                     usage='%(prog)s [options]')
    parser.add_argument("-s", action="store_true", help="Include this to scrape all sites and update googlesheets")
    parser.add_argument("-g", action='store_true', help="Include this to group all sites")
    args = parser.parse_args()
    return parser, args


def main():
    parser, args = setup_parser()
    sheet = get_sheet()

    if args.s and not args.g:
        update_google_sheets(sheet, scrape_sites())
        update_cell(sheet, "TimeStamps")
    elif args.g and not args.s:
        update_google_sheets(sheet, group_pages())
        update_cell(sheet, "TimeStamps")
    elif args.s and args.g:
        update_google_sheets(sheet, scrape_sites())
        update_google_sheets(sheet, group_pages())
        update_cell(sheet, "TimeStamps")
    else:
        print(parser.print_help())
        sys.exit(0)

if __name__ == "__main__":
    main()


