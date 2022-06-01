from race.src.raceDataConverter import race_data_converter
from horse.src.horseDataConverter import horse_data_converter
from peds.src.pedsDataConverter import peds_data_converter
import requests
import os

def slack_notifier(text):
    slack_url = os.environ["SLACK_URL"]
    request_body = {
        "text": text
    }
    request_header = {
        'content-type': 'application/json'
    }

    return requests.post(slack_url, headers=request_header, json=request_body)



if __name__ == "__main__":
    IS_TEST = False

    if IS_TEST: slack_notifier("↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓this message is test↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓")
    print("converting race data")
    race_convert_error_dict = race_data_converter(is_test=IS_TEST)

    race_message = """
    convert race data is completed !
    the number of convert error is {}.
    """.format(len(race_convert_error_dict["error_list"]))

    slack_notifier(race_message)

    print("converting horse data")
    horse_convert_error_dict = horse_data_converter(is_test=IS_TEST)

    horse_message = """
    convert horse data is completed !
    the number of convert error is {}.
    """.format(len(horse_convert_error_dict["sucess"]))

    slack_notifier(horse_message)

    print("converting peds data")
    peds_convert_error_dict = peds_data_converter(is_test=IS_TEST)

    peds_message = """
    convert peds data is completed !
    the number of convert error is {}.
    """.format(len(peds_convert_error_dict["success_id_list"]))

    slack_notifier(peds_message)
    if IS_TEST: slack_notifier("↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑this message is test↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")