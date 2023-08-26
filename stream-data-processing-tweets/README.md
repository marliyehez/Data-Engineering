# Real-time Tweets Data Stream Processing Simulation
<p align="center">
  <img src="img/stream.png" width="700">
<p>

## Description

This project focuses on real-time stream data processing, specifically tweets data. The process unfolds in the following sequence:

**1. Creation of Twitter App Simulation using Python:**  
Python is utilized to construct a "Twitter app simulation." This simulation mimics real-time data by extracting a row from a CSV table and sending it every second. The aim is to closely emulate the behavior of real-time data generation.

**2. Data Processing with Kafka:**  
The project involves the use of Kafka, a distributed streaming platform. A Kafka producer is employed to send the simulated tweets data to a Kafka consumer. This enables efficient and reliable handling of streaming data.

**3. Storage in Cloud Storage (GCS) as JSON:**  
The processed data is sent to Google Cloud Storage (GCS) in JSON format. This step facilitates the temporary storage of data in a scalable and easily accessible manner.

**4. Integration with BigQuery via Cloud Function:**  
The addition of data to GCS triggers a cloud function. This cloud function automatically transfers the data from GCS to BigQuery, where it becomes part of the data warehouse. This seamless integration ensures that real-time data is efficiently stored and available for analysis.

Through the combination of Python, Kafka, Cloud Storage, and BigQuery, this project demonstrates a robust pipeline for handling real-time stream data, paving the way for dynamic insights and data-driven decision-making.
