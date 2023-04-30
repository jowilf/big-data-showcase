# Overview

I built this project to put my knowledge of Big Data Technology into practice. I have developed a complete process for capturing real-time data from an fictional eCommerce electronics store, storing it permanently in HBase, and displaying it in real-time using the visualization tool Plotly. Additionally, I have performed data analysis using Spark SQL by executing queries, storing the results in HBase, and displaying them in Plotly.

![Architecture](/images/architecture.png)

To run this project in your local machine:

- Make sure to have `Docker` & `Maven` installed on your system
- Run `./start-all.sh`
- Navigate to http://localhost:8050/

# Dataset

The dataset used in this project is sourced from Kaggle and is available at https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-electronics-store.

This dataset consists of an eCommerce events history in an electronics store, where each row represents an event. The events can be classified into three types:

Events can be:

- `view` - a user viewed a product
- `cart` - a user added a product to shopping cart
- `purchase` - a user purchased a product

For this project, I extracted events related to 10 specific brands: `samsung`, `apple`, `asus`, `msi`, `gigabyte`, `dell`, `hp`, `lenovo`, `sony`, `intel`.

The final dataset can be found in the following location [/stream/dataset.csv](/stream/dataset.csv).

# Streaming Source

In order to simulate a streaming source, I have written a simple script that reads the dataset line by line and sends each event to a Kafka topic named `electronic-store`. The script is in [/stream](/stream/) folder

An example of kafka message:

```json
{
  "event_time": "2020-09-24 22:46:22 UTC",
  "event_type": "view",
  "product_id": "3605425",
  "category_id": "2144415942115852920",
  "category_code": "",
  "brand": "samsung",
  "price": "115.08",
  "user_id": "1515915625471820789",
  "user_session": "77952690-1108-4def-9016-d08b39784f17"
}
```

# Spark Streaming

Using Spark Streaming, I performed two types of aggregations on the data within 2-second time windows:

## Views per category

For the first aggregation, I counted and printed the number of views per top-level category. This information is useful for understanding the popularity of different product categories in the store and identifying trends over time.

## Event type aggregation

For the second aggregation, I count the number of `views`, `cart` additions, and `purchases` within the 2-second time windows.

The result is sent to another Kafka topic named `electronic-analytics` to be visualized in real time with Plotly.

The result is also saved into HBase for future reference. In HBase, the computed time is used as a row key and the result is saved into one column family with 3 columns when each column represent the event_type.

![](/images/hbase_events.png)

The code can be found in the file [KafkaStream.java](/src/main/java/com/jowilf/KafkaStream.java)

# Spark SQL

To perform some static analysis on the dataset, I used Spark SQL. The analysis involved counting the numbers of views, cart additions, and purchases per brand, and the results were saved into HBase.

![](/images/hbase_report.png)

Additionally, I visualized the analysis results using Plotly.

The code can be found in the file [SparkSQLAnalyze.java](/src/main/java/com/jowilf/SparkSQLAnalyze.java)

# Visualization with Plotly

[Plotly](https://plotly.com/) is an interactive data visualization tool that is used to visualize the real-time user behavior in the app and the static analysis made using Spark SQL.

## Real time event ( Spark Streaming )

![Real time event with Spark Streaming](/images/real_time_events.png)

## Events by brand ( Spark SQL )

![Events by brand with Spark SQL](/images/events_by_brands.png)
