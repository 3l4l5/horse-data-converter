from logging import getLogger
import glob
import os
import json
import re
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import sys

sys.path.append(
    os.path.join(
        os.path.dirname(__file__),
        '..',
        '..',
        '..',
        'my_packages'
        )
    )
import data_controller

logger = getLogger(__name__)

def convert_html(path:str) -> pd.DataFrame:
    """指定されたpathのhtmlから馬データのdataframeを返す

    Args:
        path (str): the file path of horse html
    Returns:
        pd.DataFrame: data frame of horse data
    """

    def get_soup(path):
        with open(path, "r") as f:
            html = f.read()
        return BeautifulSoup(html, "lxml")

    def get_main_table(soup):

        main_data = soup.find_all(class_="db_h_race_results")
        horse_data = pd.read_html(str(main_data))
        return horse_data[0]

    def convert_horse_table(table, soup):
        rename_dict = {
            '日付': 'date',
            '開催': 'place',
            '天気': 'weather',
            'レース名': 'race_name',
            '頭数': 'number_of_horse',
            '枠番': 'bracket_number',
            '馬番': 'horse_number',
            'タイム': 'time',
            '着差': 'goal_diff',
            '通過': 'pass',
            'ペース': 'pase',
            '上り': 'up_time',
            '賞金': 'prize',
            'オッズ': 'odds',
            '人気': 'popularity',
            '着順': 'order',
            '斤量': 'load_weight',
            '馬場': 'cource_status'
        }
        drop_column_list = [
            '映像',
            '距離',
            '馬場指数',
            'ﾀｲﾑ指数',
            '厩舎ｺﾒﾝﾄ',
            '備考',
            '馬体重',
            '騎手',
            '勝ち馬(2着馬)',
        ]

        soup_str = str(soup.find(class_="db_h_race_results"))
        # race_idを取得
        race_id = re.findall(r"(?<=/race/)[0-9][0-9a-zA-Z]*?(?=/)", soup_str)
        table["race_id"] = race_id
        # jockey_idを取得
        jockey_id = re.findall(r"(?<=/jockey/result/recent/)[0-9a-zA-Z]*?(?=/)", soup_str)
        table["jockey_id"] = jockey_id

        table["length"] = table["距離"].map(lambda x: re.findall(r"\d+", x)[0])
        table["race_type"] = table["距離"].map(lambda x: x[0])

        # 歳を計算
        birth_date = pd.read_html(str(soup.find(class_="db_prof_table")),index_col=0)[0].loc["生年月日"].values[0]
        birth_date_datetime = datetime.strptime(birth_date, '%Y年%m月%d日')
        table["age"] = table["日付"].map(lambda date_str: (datetime.strptime(date_str, '%Y/%m/%d') - birth_date_datetime).days)

        # 馬体重変換
        table["horse_weight"] = table["馬体重"].map(lambda x: None if x=="計不" else x.split("(")[0] if "(" in x else x)
        table["horse_weight_diff"] = table["馬体重"].map(lambda x: None if x=="計不" else x.split("(")[1][:-1] if "(" in x else 0)

        # winhorse
        a_list = soup.find(class_="db_h_race_results").find_all("a")

        horse_id_list = []
        for a in a_list:
            href = str(a.get("href"))
            id = re.findall(r"\d+", href)[0]
            if "/horse/" in href:
                horse_id_list.append(id)
        # 勝ち馬カラムのうち、NaNとなっているもののindex番号を取得
        winhorse_null_bool = list(table["勝ち馬(2着馬)"].isnull())
        if sum(winhorse_null_bool):
            buf = []
            c = 0
            for tf in winhorse_null_bool:
                if tf:
                    buf.append(np.nan)
                else:
                    buf.append(horse_id_list[c])
                    c += 1
            horse_id_list = buf

        table["win_horse"] = horse_id_list

        # 賞金カラムからnanを削除
        table["賞金"] = table["賞金"].fillna(0)

        # 不要列削除
        table = table.drop(columns=drop_column_list)
        # 列名変更
        table = table.rename(columns=rename_dict)
        return table

    soup = get_soup(path)
    main_table = get_main_table(soup)
    return convert_horse_table(main_table, soup)


def save_csv(save_path, save_data:pd.DataFrame):
    try:
        save_data.to_csv(save_path)
    except Exception as e:
        logger.error(e)


def save_log(save_path, save_data:dict):
    try:
        with open(save_path, "w") as f:
            json.dump(save_data, f, indent=4)
    except Exception as e:
        logger.error(e)



def main():
    DRY_RUN = False
    RAW_DATA_DIR = os.path.join("/nas", "project", "horse", "data", "horse")
    CSV_DATA_DIR = os.path.join("/nas", "project", "horse", "csvs", "horse")

    dc = data_controller.DataController()
    dc.connect()

    # スクレイピングのログと変換のログを見比べて、まだ変換していない馬のIDを取得する。
    scrape_log_list = [os.path.basename(path) for path in glob.glob(os.path.join(RAW_DATA_DIR, "log", "*"))]
    convert_log_list = [os.path.basename(path) for path in glob.glob(os.path.join(CSV_DATA_DIR, "log", "*"))]


    target_log_list = list(set(scrape_log_list) - set(convert_log_list))
    target_log_path_list = [os.path.join(RAW_DATA_DIR, "log", filename) for filename in target_log_list]
    log_json_list = []
    for path in target_log_path_list:
        with open(path, "r") as f:
            txt = f.read()
            if txt!="":
                log_json_list.append(json.loads(txt))
    for log in log_json_list:

        target_horse_ids = log["horse_id"]
        logger.info("converting {}".format(log))
        for horse_id in tqdm(sorted(set(target_horse_ids))[:100]):
            raw_data_list = glob.glob(os.path.join(RAW_DATA_DIR, "data", horse_id, "*"))
            raw_data_path = sorted(raw_data_list)[-1]
            try:
                if DRY_RUN:
                    pass
                else:
                    table = convert_html(raw_data_path)
                    dc.save("csvs/horse/data/{}.csv".format(horse_id),table)
                    input()
            except Exception as e:
                print(raw_data_path)
                logger.error(e)



if __name__ == "__main__":
    main()