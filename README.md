# Real-Time Anomaly Detection Pipeline for Stock Trading Data with Redpanda and Quix

![project overview](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*oKfAPrtX8XzWbVLqncfebA.jpeg)

This project is a real-time anomaly detection system for stock trading data, built with Redpanda and Quix. It processes streaming trade data and flags unusual patterns using both rule-based and machine learning techniques.  

## Purpose  
The goal is to detect anomalies—like sudden price jumps or high-volume trades—as they happen, using a lightweight, practical setup.  

## Tech Stack  
- **Redpanda**: Streams the data (Kafka-compatible).  
- **Quix**: Manages the pipeline.  
- **Python**: Powers the processing and ML (Pandas, NumPy, Scikit-learn).  
- **Docker Compose**: Runs the environment.  


## Details  
- **Data**: Streams simulated stock trades from a CSV.  
- **Anomaly Detection**: Uses volume thresholds, price change rules, and an Isolation Forest model.  
- **Article**: Read more [here](https://medium.com/@jushijun/building-a-real-time-anomaly-detection-pipeline-for-stock-trading-data-with-redpanda-and-quix-83da5a013599).  

## References

- YouTube @CodeWithYu https://www.youtube.com/watch?v=RUfVVOhihEA&t=2s&ab_channel=CodeWithYu
- https://github.com/airscholar/RealtimeAnomalyDetection