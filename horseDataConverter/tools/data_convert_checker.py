import glob
import os

if __name__ == "__main__":
    mount_point = os.environ["MOUNT_POINT"]
    horse_csv_dirs = glob.glob(os.path.join(mount_point, "csvs", "horse", "data", "*"))
    horse_html_dirs = glob.glob(os.path.join(mount_point, "data", "horse", "data", "*"))
    print(len(horse_csv_dirs))
    print(len(horse_html_dirs))
    race_csv_dirs = glob.glob(os.path.join(mount_point, "csvs", "race", "*"))
    race_html_dirs = glob.glob(os.path.join(mount_point, "data", "race", "????", "*", "*", "*"))
    print(len(race_csv_dirs))
    print(len(race_html_dirs))
    peds_csv_dirs = glob.glob(os.path.join(mount_point, "csvs", "peds", "*"))
    peds_html_dirs = glob.glob(os.path.join(mount_point, "data", "peds", "*"))
    print(len(peds_csv_dirs))
    print(len(peds_html_dirs))