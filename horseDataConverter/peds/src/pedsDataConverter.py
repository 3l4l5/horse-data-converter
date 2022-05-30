from logging import getLogger
import glob
import os
import json
from tqdm import tqdm
from bs4 import BeautifulSoup

logger = getLogger(__name__)


def _get_peds_id_list(soup):
    horse_id_list = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if not "/horse/ped/" in href and not "/horse/sire/" in href and not "/horse/mare/" in href:
            horse_id = href.split("/")[-2]
            horse_id_list.append(horse_id)
    return horse_id_list


def _get_peds_dict(horse_id_list):
    # HACK: 絶対もっといい書き方ある。
    # NOTE: 1 が父を示し、２が母を占めす
    peds_dict = {
        1 : horse_id_list[0],
        11 : horse_id_list[1],
        111 : horse_id_list[2],
        1111 : horse_id_list[3],
        11111 : horse_id_list[4],
        11112 : horse_id_list[5],
        1112 : horse_id_list[6],
        11121 : horse_id_list[7],
        11122 : horse_id_list[8],
        112 : horse_id_list[9],
        1121 : horse_id_list[10],
        11211 : horse_id_list[11],
        11212 : horse_id_list[12],
        1122 :  horse_id_list[13],
        11221 :  horse_id_list[14],
        11222 :  horse_id_list[15],
        12 : horse_id_list[16],
        121 : horse_id_list[17],
        1211 : horse_id_list[18],
        12111 : horse_id_list[19],
        12112 : horse_id_list[20],
        1212 : horse_id_list[21],
        12121 : horse_id_list[22],
        12122 : horse_id_list[23],
        122 : horse_id_list[24],
        1221 : horse_id_list[25],
        12211 : horse_id_list[26],
        12212 : horse_id_list[27],
        1222 : horse_id_list[28],
        12221 : horse_id_list[29],
        12222 : horse_id_list[30],
        2 : horse_id_list[31],
        21 : horse_id_list[32],
        211 : horse_id_list[33],
        2111 : horse_id_list[34],
        21111 : horse_id_list[35],
        21112 : horse_id_list[36],
        2112 : horse_id_list[37],
        2112 : horse_id_list[38],
        21122 : horse_id_list[39],
        212 : horse_id_list[40],
        2121 : horse_id_list[41],
        21211 : horse_id_list[42],
        21212 : horse_id_list[43],
        2122 :  horse_id_list[44],
        21221 :  horse_id_list[45],
        21222 :  horse_id_list[46],
        22 : horse_id_list[47],
        221 : horse_id_list[48],
        2211 : horse_id_list[49],
        22111 : horse_id_list[50],
        22112 : horse_id_list[51],
        2212 : horse_id_list[52],
        22121 : horse_id_list[53],
        22122 : horse_id_list[54],
        222 : horse_id_list[55],
        2221 : horse_id_list[56],
        22211 : horse_id_list[57],
        22212 : horse_id_list[58],
        2222 : horse_id_list[59],
        22221 : horse_id_list[60],
        22222 : horse_id_list[61],
    }
    peds_tuple = sorted(peds_dict.items(), key=lambda i: i[0])
    peds_dict = {peds[0]:peds[1] for peds in peds_tuple}
    return peds_dict


def get_peds_dict(peds_html_path):
    with open(peds_html_path) as f:
        html_text = f.read()
    soup = BeautifulSoup(html_text, "lxml")
    peds_main_table_soup = soup.find_all(class_="blood_table")
    horse_id_list = _get_peds_id_list(peds_main_table_soup[0])
    peds_dict = _get_peds_dict(horse_id_list)
    return peds_dict


def save_dict(peds_dict, save_path):
    with open(save_path, "w") as f:
        json.dump(peds_dict ,f, indent=4)
    return True


def get_horse_id_list(dir_path):
    file_path_list = glob.glob(os.path.join(dir_path, "*"))
    id_list = [os.path.basename(path).split(".")[0] for path in file_path_list]
    return id_list


def peds_data_converter(is_test):
    mount_point = os.environ["MOUNT_POINT"]
    if not is_test:
        PEDS_HTML_DIR = os.path.join(mount_point, "data", "peds")
        PEDS_CSV_DIR  = os.path.join(mount_point, "csvs", "peds")
    else:
        PEDS_HTML_DIR = os.path.join(mount_point, "test", "data", "peds")
        PEDS_CSV_DIR  = os.path.join(mount_point, "test", "csvs", "peds")

    if not os.path.exists(PEDS_CSV_DIR):
        os.makedirs(PEDS_CSV_DIR, exist_ok=True)

    already_got_horseid = get_horse_id_list(PEDS_CSV_DIR)
    all_horse_id        = get_horse_id_list(PEDS_HTML_DIR)
    target_horse_id_list = list(set(all_horse_id) - set(already_got_horseid))

    error_horse_ids = []
    sucess_horse_ids = []
    for horse_id in tqdm(target_horse_id_list):
        try:
            peds_html_path = os.path.join(PEDS_HTML_DIR, horse_id+".html")
            peds_dict = get_peds_dict(peds_html_path)
            peds_json_path = os.path.join(PEDS_CSV_DIR, horse_id+".json")
            save_dict(peds_dict, peds_json_path)
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
