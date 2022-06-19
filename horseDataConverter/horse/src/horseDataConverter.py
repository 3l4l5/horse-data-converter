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

logger = getLogger(__name__)

def convert_html(path: str) -> pd.DataFrame:
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


def save_csv(save_path, save_data: pd.DataFrame):
    try:
        save_data.to_csv(save_path)
    except Exception as e:
        logger.error(e)


def save_log(save_path, save_data: dict):
    try:
        with open(save_path, "w") as f:
            json.dump(save_data, f, indent=4)
    except Exception as e:
        logger.error(e)



def horse_data_converter(is_test: bool) -> dict:
    """実際に変換する関数

    Args:
        is_test (bool): テストならTrue。保存先などが変わります

    Returns:
        dict: 成功したidを失敗したidを返す
    """
    IS_TEST = is_test
    NOW = datetime.now().strftime('%Y-%m-%d-%H-%M')
    mount_point = os.environ["MOUNT_POINT"]
    DRY_RUN = False
    if not IS_TEST:
        RAW_DATA_DIR = os.path.join(mount_point, "data", "horse")
        CSV_DATA_DIR = os.path.join(mount_point, "csvs", "horse")
    else:
        RAW_DATA_DIR = os.path.join(mount_point, "test", "data", "horse")
        CSV_DATA_DIR = os.path.join(mount_point, "test", "csvs", "horse")

    HORSE_CSV_DATA_DIR = os.path.join(CSV_DATA_DIR, "data")
    CSV_LOG_DIR  = os.path.join(CSV_DATA_DIR, "log")

    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
    if not os.path.exists(HORSE_CSV_DATA_DIR):
        os.makedirs(HORSE_CSV_DATA_DIR)
    if not os.path.exists(CSV_LOG_DIR):
        os.makedirs(CSV_LOG_DIR)

    # スクレイピングのログと変換のログを見比べて、まだ変換していない馬のIDを取得する。
    scrape_log_list = [os.path.basename(path) for path in glob.glob(os.path.join(RAW_DATA_DIR, "log", "*"))]
    convert_log_list = [os.path.basename(path) for path in glob.glob(os.path.join(CSV_LOG_DIR, "*"))]

    target_log_list = list(set(scrape_log_list) - set(convert_log_list))
    target_log_path_list = [os.path.join(RAW_DATA_DIR, "log", filename) for filename in target_log_list]

    log_json_dict = {}
    for path in target_log_path_list:
        with open(path, "r") as f:
            txt = f.read()
            if txt!="":
                log_json_dict[path] = json.loads(txt)

    error_path_list = []
    for path in log_json_dict:

        log = log_json_dict[path]

        splited_path = path.split(os.sep)
        splited_path[-4] = "csvs"
        horse_log_path = os.sep.join(splited_path)

        target_horse_ids = log["horse_id"]
        logger.info("converting {}".format(log))

        error_is_list = []
        success_id_list = []
        for horse_id in tqdm(sorted(set(target_horse_ids))):
            raw_data_list = glob.glob(os.path.join(RAW_DATA_DIR, "data", horse_id, "*"))
            raw_data_path = sorted(raw_data_list)[-1]
            try:
                save_dir_path = os.path.join(HORSE_CSV_DATA_DIR, horse_id)
                if not os.path.exists(save_dir_path):
                    os.makedirs(save_dir_path)

                if not DRY_RUN:
                    table = convert_html(raw_data_path)
                    table.to_csv(os.path.join(save_dir_path, "{}.csv".format(NOW)), index=False, header=True)
                else:
                    pass
                success_id_list.append(horse_id)

            except Exception as e:
                print(raw_data_path)
                error_path_list.append(raw_data_path)
                logger.error(e)
                error_is_list.append(horse_id)

        result_dict = {
            "sucess": success_id_list,
            "failed": error_is_list
        }
        save_log(horse_log_path, result_dict)
    return error_path_list


def horse_data_converter(is_test: bool) -> dict:
    """実際に変換する関数

    Args:
        is_test (bool): テストならTrue。保存先などが変わります

    Returns:
        dict: 成功したidを失敗したidを返す
    """
    mount_point = os.environ["MOUNT_POINT"]
    IS_TEST = is_test
    NOW = datetime.now().strftime('%Y-%m-%d-%H-%M')
    DRY_RUN = False
    if not IS_TEST:
        RAW_DATA_DIR = os.path.join(mount_point, "data", "horse")
        CSV_DATA_DIR = os.path.join(mount_point, "csvs", "horse")
    else:
        RAW_DATA_DIR = os.path.join(mount_point, "test", "data", "horse")
        CSV_DATA_DIR = os.path.join(mount_point, "test", "csvs", "horse")

    HORSE_CSV_DATA_DIR = os.path.join(CSV_DATA_DIR, "data")
    CSV_LOG_DIR  = os.path.join(CSV_DATA_DIR, "log")

    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
    if not os.path.exists(HORSE_CSV_DATA_DIR):
        os.makedirs(HORSE_CSV_DATA_DIR)
    if not os.path.exists(CSV_LOG_DIR):
        os.makedirs(CSV_LOG_DIR)

    all_horse_dir_list = glob.glob(os.path.join(RAW_DATA_DIR, "data", "*"))
    success_id_list = []
    error_id_list = []

    for horse_dir in tqdm(all_horse_dir_list):
        horse_id = horse_dir.split(os.sep)[-1]
        html_path_list = glob.glob(os.path.join(horse_dir, "*"))
        # NOTE: horseの情報の中で最新のものを取得
        ratest_html_path = sorted(html_path_list)[-1]
        ratest_csv_path = ratest_html_path.replace("/data/horse", "/csvs/horse").replace(".html", ".csv")
        save_dir_name = "/".join(ratest_csv_path.split(os.sep)[:-1])
        if not os.path.exists(ratest_csv_path):
            if not os.path.exists(save_dir_name):
                os.makedirs(save_dir_name)
                try:
                    table = convert_html(ratest_html_path)
                    table.to_csv(ratest_csv_path, index=False, header=True)
                    success_id_list.append(horse_id)
                except Exception as e:
                    logger.error(e)
                    print(horse_id)
                    error_id_list.append(horse_id)

    result_dict = {
        "sucess": success_id_list,
        "failed": error_id_list
        }
    with open(os.path.join(CSV_LOG_DIR, NOW+".json"), "w") as f:
        json.dump(result_dict, f, indent=4)
    return result_dict


if __name__ == "__main__":
    horse_data_converter(is_test=True)
