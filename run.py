import datetime
import json
import os
import time

import pandas
import yaml
from confluent_kafka import Consumer, KafkaException


def main(config):
    """
    The main program loop which reads in Kafka items and processes them

    Args:
        config (pyyaml config): The main config file for this application
    """
    consumer = Consumer(
        {
            "bootstrap.servers": config["kafka"][0]["bootstrap-servers"],
            "group.id": "video-event-csv-converter",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([config["kafka"][0]["topic"]])
    cols = [
        "initial_timestamp",
        "label",
        "hit_count",
        "pole_id",
        "camera_id",
        "intersection",
    ]
    for i in range(50):
        cols.append("timestamp_{}".format(i))
        cols.append("x1_{}".format(i))
        cols.append("y1_{}".format(i))
        cols.append("x2_{}".format(i))
        cols.append("y2_{}".format(i))

    num_cols = len(cols)

    dataframes = {
        "mlk-peeples-cam-1": {},
        "mlk-central-cam-1": {},
        "mlk-georgia-cam-3": {},
        "mlk-houston-cam-1": {},
    }

    while True:
        try:
            consumer.poll(1)
            msg = consumer.consume()
            # Get data from Kafka
            if msg is not None:
                for m in msg:
                    if m.error():
                        continue
                    j = json.loads(m.value())

                    # Ignore certain cameras
                    if not j["camera_id"] in dataframes:
                        continue

                    converted_time = datetime.datetime.utcfromtimestamp(
                        j["timestamp"] / 1000
                    )

                    date = converted_time.date()
                    key = "{}_{}_{}".format(date.month, date.day, date.year)

                    if not key in dataframes[j["camera_id"]]:
                        dataframes[j["camera_id"]][key] = pandas.DataFrame(columns=cols)

                    fixed = [
                        j["timestamp"],
                        j["label"],
                        j["hit_counts"],
                        j["pole_id"],
                        j["camera_id"],
                        j["intersection"],
                    ]
                    variable = []

                    for location in j["locations"]:
                        variable.append(location["timestamp"])
                        variable.append(int(location["coords"][0]))
                        variable.append(int(location["coords"][1]))
                        variable.append(int(location["coords"][2]))
                        variable.append(int(location["coords"][3]))

                    # Pad with spaces
                    while len(variable) < (num_cols - len(fixed)):
                        variable.append("")

                    dataframes[j["camera_id"]][key].loc[
                        dataframes[j["camera_id"]][key].shape[0]
                    ] = (fixed + variable)
                    dataframes[j["camera_id"]][key].to_csv("{}-{}.csv".format(j["camera_id"], key))

        except KeyboardInterrupt:
            break
    consumer.close()


if __name__ == "__main__":
    if os.path.exists("config.yaml"):
        with open("config.yaml") as file:
            config = yaml.load(file.read(), Loader=yaml.Loader)
        main(config)
    else:
        print("No config file found")
        exit()
