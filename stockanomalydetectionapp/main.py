# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application

import os
import json
import glob
import tqdm
import pandas as pd

# for local dev, you can load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="data_producer", 
        auto_create_topics=True,
        broker_address='localhost:19092')
    
    topic_name = os.environ["output"]
    topic = app.topic(name=topic_name)

    # num_loaded = 0

    # create a pre-configured Producer object.
    with app.get_producer() as producer:
        # iterate over the data from the hardcoded dataset
        files = glob.glob('data/*.zst')
        files.sort()

        for file_path in tqdm.tqdm(files):
            print(f'Processing file: {file_path}')

            data = pd.read_csv(file_path)

            for _, row in data.iterrows():
                trade = row.to_dict()

                json_data = json.dumps(trade)  # convert the row to JSON

                # publish the data to the topic
                producer.produce(
                    topic=topic.name,
                    key=trade['symbol'],
                    value=json_data,
                )
            #     num_loaded += 1

            #     if num_loaded >=10005: # restrict number of loaded to 10005
            #         break
            # if num_loaded >=10005:
            #     break

if __name__ == "__main__":
    main()