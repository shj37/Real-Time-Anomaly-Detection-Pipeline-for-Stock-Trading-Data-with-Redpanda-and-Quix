import os
from collections import defaultdict, deque

import json
import numpy as np
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
from quixstreams import Application
from sklearn.ensemble import IsolationForest

# for local dev, you can load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(
    consumer_group="anomaly_detector", 
    auto_create_topics=True,
    auto_offset_reset="earliest",
    broker_address='localhost:19092')

input_topic = app.topic(os.environ["input_topic"]) # trade data topic
output_topic = app.topic(os.environ["output_topic"]) # detected anomaly topic

processed_sequence = set()

high_volume_threshold = defaultdict(lambda: 5000)
rapid_price_move_threshold = defaultdict(lambda: 0.02) # change by 2%
trade_history = defaultdict(lambda: deque(maxlen=100))

isolation_forest = IsolationForest(contamination=0.01, n_estimators=1000)
fit_prices_latest = []
is_fitted = False # record whether isolation model is ready (trained)

def high_volume_check(trade_data, volume_threshold):
    return trade_data['size'] > volume_threshold[trade_data['symbol']]

def rapid_price_move_check(trade_data, trade_history, rapid_price_move_threshold):
    symbol = trade_data['symbol']
    current_price = trade_data['price']

    if not trade_history[symbol]:
        trade_history[symbol].append(current_price)
        return False
    
    last_price = trade_history[symbol][-1]
    price_change = abs((current_price - last_price)/last_price)
    trade_history[symbol].append(current_price)
    
    return price_change > rapid_price_move_threshold[symbol]

# algo to detect price anomalies
def isolation_forest_check(trade_data):
    global is_fitted
    global fit_prices_latest
    current_price = trade_data['price']

    fit_prices_latest.append(float(current_price))

    if len(fit_prices_latest) < 10000:
        return False


    if len(fit_prices_latest) % 10000 == 0:
        # only save and train the latest 1000 trade data to save space
        fit_prices_latest = fit_prices_latest[-10000:]
                
        fit_prices_normalised = (np.array(fit_prices_latest) - float(np.mean(fit_prices_latest))) / float(np.std(fit_prices_latest))
        prices_reshaped = fit_prices_normalised.reshape(-1, 1)
        isolation_forest.fit(prices_reshaped)
        is_fitted = True

    if not is_fitted:
        return False

    current_price_normalised = (current_price - float(np.mean(fit_prices_latest))) / float(np.std(fit_prices_latest))
    score = isolation_forest.decision_function([[current_price_normalised]])

    if score[0] < 0:
        print(f"DECTECTION SCORE: {score[0]}")

    return score[0] < 0


def process_data(trade_data):
    sequence = trade_data['sequence']

    if sequence in processed_sequence:
        return

    processed_sequence.add(sequence)

    with app.get_producer() as producer:
        anomalies = []

        if high_volume_check(trade_data, high_volume_threshold):
            anomalies.append("High Volume")
        
        if rapid_price_move_check(trade_data, trade_history,rapid_price_move_threshold):
            anomalies.append("Rapid Price Movement")

        if isolation_forest_check(trade_data):
            anomalies.append("Isolation Forest Anomalies")

        if anomalies:
            trade_data["anomalies"] = anomalies

            print(f"Anomalies detected: {trade_data}")

            # push the anomaly record to output topic
            anomaly_data = json.dumps(trade_data)

            producer.produce(
                topic=output_topic.name,
                key=str(trade_data['sequence']),
                value=anomaly_data,
            )

def main():
    stream_df = app.dataframe(input_topic) # convert streaming input data into dataframe - previously dictionary

    # processing data, detecting anomalies
    stream_df = stream_df.apply(process_data) 

    # load processed data / result to output_topic
    # stream_df.to_topic(output_topic)

    # run the app - produce result data
    app.run(stream_df)






if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting Streaming!")






