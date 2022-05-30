import glob
import os
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import re
from logging import getLogger

IS_TEST = True

logger = getLogger(__name__)


def convertRaceHtml(path):
    def get_soup(path):
        # htmlを読み込み
        with open(path, "r") as f:
            html = f.read()
        return BeautifulSoup(html, "lxml")

    def get_result_table(soup):
        race_result = soup.find(class_="race_table_01")
        result_table = pd.read_html(str(race_result))[0]

        # read_htmlで得られる情報を加工
        result_table["order"] = result_table["着順"]
        result_table["bracket_number"] = result_table["枠番"]
        result_table["horse_number"] = result_table["馬番"]
        result_table["horse_name"] = result_table["馬名"]
        result_table["horse_sex"] = result_table["性齢"].map(lambda x: x[0])
        result_table["horse_age"] = result_table["性齢"].map(lambda x: x[1:])
        result_table["load_weight"] = result_table["斤量"]
        result_table["jockey_name"] = result_table["騎手"]
        result_table["odds"] = result_table["単勝"]
        result_table["popularity"] = result_table["人気"]
        result_table["horse_weight"] = result_table["馬体重"]\
            .map(lambda x: None if x == "計不" else x.split("(")[0])
        result_table["horse_weight_diff"] = result_table["馬体重"]\
            .map(lambda x: None if x == "計不" else x.split("(")[1][:-1])
        result_table["trainer"] = result_table["調教師"]
        result_table["time"] = result_table["タイム"]
        result_table["goal_diff"] = result_table["着差"]

        # 不必要なカラムを削除
        result_table.drop(
            columns=[
                "着順",
                "枠番",
                "馬番",
                "馬名",
                "性齢",
                "斤量",
                "騎手",
                "単勝",
                "人気",
                "馬体重",
                "調教師",
                "タイム",
                "着差"
                ], inplace=True)

        # htmlから得られる情報を付加
        race_info = soup.find(class_="racedata").p.span.string.split("/")
        # 競馬場の名前を取得
        result_table["racecourse"] = soup.find(class_="race_place").find(class_="active").string
        # レース番号を取得
        result_table["race_round"] = soup.find(class_="race_num").find(class_="active").string[:-1]

        # 馬場の種類について
        cource_info = race_info[0]
        # 障害 = 1 一般レース = 0
        result_table["hurdle_race"] = 0 if "障" in cource_info else 1
        # 芝 = 1 ダート = 0 両方 = 2
        result_table["grass_race"] = 0 if "芝" not in cource_info else 1 if "ダ" not in cource_info else 2
        # 距離
        result_table["length"] = re.findall(r"\d+", cource_info)[0]
        # 左右
        result_table["left_right"] = "left" if "左" in cource_info else "right" if "右" in cource_info else "-"

        # 天候
        weather_info = race_info[1]
        result_table["weather"] = re.findall(r"(?<=天候 : ).+", weather_info)[0]

        # 馬場の状態
        cource_status = race_info[2]
        if "ダート" not in cource_status:
            result_table["dart_cource_status"] = "-"
            result_table["grass_cource_status"] = re.findall(r"(?<=芝 : ).+", cource_status)[0]
        elif "芝" not in cource_status:
            result_table["dart_cource_status"] = re.findall(r"(?<=ダート : ).+", cource_status)[0]
            result_table["grass_cource_status"] = "-"
        else:
            result_table["dart_cource_status"] = re.findall(r"(?<=ダート : ).+", cource_status)[0]
            result_table["grass_cource_status"] = re.findall(r"(?<=芝 : ).+", cource_status)[0]

        # 発走時間
        time_info = race_info[3]
        result_table["start_time"] = re.findall(r"(?<=発走 : ).+", time_info)[0]

        # 各IDを取得
        a_list = soup.find(class_="race_table_01").find_all("a")
        horse_id_list = []
        jockey_id_list = []
        trainer_id_list = []
        owner_id_list = []
        for a in a_list:
            href = str(a.get("href"))
            id = re.findall(r"\d+", href)[0]
            if "/horse/" in href:
                horse_id_list.append(id)
            if "/jockey/" in href:
                jockey_id_list.append(id)
            if "/trainer/" in href:
                trainer_id_list.append(id)
            if "/owner/" in href:
                owner_id_list.append(id)
        # horseID
        result_table["horse_id"] = horse_id_list
        # jockey ID
        result_table["hockey_id"] = jockey_id_list
        # trainer ID
        result_table["trainer_id"] = trainer_id_list
        # owner ID
        result_table["owner_id"] = owner_id_list

        # レースの最大賞金
        result_table["max_prize"] = soup.find(class_="race_table_01")\
            .find_all("tr")[1]\
            .find_all("td")[-1]\
            .string.replace(",", "")
        # 各馬の獲得賞金
        prize_list = []
        for tr in soup.find(class_="race_table_01").find_all("tr")[1:]:
            prize = tr.find_all("td")[-1].string
            if prize is None:
                prize_list.append("0")
            elif "," in prize:
                prize_list.append(prize.replace(",", ""))
            else:
                prize_list.append(prize)
        result_table["prize"] = prize_list
        return result_table

    def get_payback_table(soup):
        pay_back_tables = soup.find(class_="pay_block").find_all(class_="pay_table_01")
        pay_back_table0 = pay_back_tables[0]
        pay_back_table1 = pay_back_tables[1]

        pay_table0 = pd.read_html(str(pay_back_table0).replace("<br>", "br").replace("<br/>", "br"))[0]
        pay_table1 = pd.read_html(str(pay_back_table1).replace("<br>", "br").replace("<br/>", "br"))[0]
        pay_table = pd.concat([pay_table0, pay_table1])
        pay_table = pay_table.rename(columns={0:"", 1:"win_horse", 2:"payback", 3:"win_fav"})
        return pay_table

    soup = get_soup(path)
    result_table = get_result_table(soup)
    payback_table = get_payback_table(soup)

    return result_table, payback_table


def race_data_converter(is_test):
    mount_point = os.environ["MOUNT_POINT"]
    IS_TEST = is_test

    if not IS_TEST:
        save_dir_race = os.path.join(mount_point, "csvs", "race")
        save_dir_payback = os.path.join(mount_point, "csvs", "payback")
    else:
        save_dir_race = os.path.join(mount_point, "test",  "csvs", "race")
        save_dir_payback = os.path.join(mount_point, "test", "csvs", "payback")

    if not os.path.exists(save_dir_race):
        os.makedirs(save_dir_race)
    if not os.path.exists(save_dir_payback):
        os.makedirs(save_dir_payback)

    race_html_list = glob.glob(os.path.join(mount_point, "data", "race", "*", "*", "*", "*.html"))
    converted_race_csv_path = glob.glob(os.path.join(save_dir_race, "*.csv"))
    converted_race_id_list = [os.path.basename(path).split(".")[0] for path in converted_race_csv_path]

    target_race_html_list = []
    for html_path in race_html_list:
        html_race_id = os.path.basename(html_path).split(".")[0]
        if not html_race_id in converted_race_id_list:
            target_race_html_list.append(html_path)

    error_list = []
    for html_path in tqdm(target_race_html_list):
        try:
            result_table, payback_table = convertRaceHtml(html_path)
            race_id = os.path.basename(html_path).split(".")[0]
            save_path_race = os.path.join(save_dir_race, race_id+".csv")
            save_path_payback = os.path.join(save_dir_payback, race_id+".csv")
            result_table.to_csv(save_path_race, index=False, header=True)
            payback_table.to_csv(save_path_payback, index=False, header=True)
        except Exception as e:
            error_list.append(html_path)
            logger.error("Error is occured in {}.".format(html_path))
            logger.error(e)
    return {"error_list": error_list}


if __name__ == "__main__":
    race_data_converter(is_test=True)