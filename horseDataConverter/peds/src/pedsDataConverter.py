from logging import getLogger
import glob
import os
import json
import re
import types
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np

logger = getLogger(__name__)

def _get_peds_id_list(soup):
    horse_id_list = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if not "/horse/ped/" in href and not "/horse/sire/" in href and not "/horse/mare/" in href:
            horse_id = href.split("/")[-2]
            horse_id_list.append(horse_id)
    # 順番がバラバラ。並び替え必要
    return horse_id_list


def get_peds_table(peds_html_path):
    with open(peds_html_path) as f:
        html_text = f.read()
    soup = BeautifulSoup(html_text, "lxml")
    peds_main_table_soup = soup.find_all(class_="blood_table")
    peds_id_list = _get_peds_id_list(peds_main_table_soup[0])
    print(peds_id_list)
    print(peds_html_path)
    input()

    #peds_table_df = pd.read_html(peds_main_table_str)[0]
    #print(peds_table_df[0])

    return ""


def save_table(table):
    return ""

def get_horse_id_list(dir_path):
    file_path_list = glob.glob(os.path.join(dir_path, "*"))
    id_list = [os.path.basename(path).split(".")[0] for path in file_path_list]
    return id_list

def peds_data_converter(is_test):
    mount_point = os.environ["MOUNT_POINT"]
    if not is_test:
        PEDS_HTML_DIR = os.path.join(mount_point, "data", "peds")
        PEDS_CSV_DIR = os.path.join(mount_point, "csvs", "peds")
    else:
        PEDS_HTML_DIR = os.path.join(mount_point, "test", "data", "peds")
        PEDS_CSV_DIR = os.path.join(mount_point, "test", "csvs", "peds")

    already_got_horseid = get_horse_id_list(PEDS_CSV_DIR)
    all_horse_id        = get_horse_id_list(PEDS_HTML_DIR)
    target_horse_id_list = list(set(all_horse_id) - set(already_got_horseid))

    error_horse_ids = []
    sucess_horse_ids = []
    for horse_id in target_horse_id_list:
        try:
            peds_html_path = os.path.join(PEDS_HTML_DIR, horse_id+".html")
            peds_table = get_peds_table(peds_html_path)
            save_table(peds_table)
            sucess_horse_ids.append(horse_id)
        except Exception as e:
            logger.error(e)
            error_horse_ids.append(horse_id)

    result_dict = {
        "success_id_list": sucess_horse_ids,
        "error_id_list": error_horse_ids
    }

    return result_dict


if __name__ == "__main__":
    peds_data_converter(is_test=True)
