package com.jowilf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SparkStreaming {
    public static String KAFKA_TOPIC = "electronic-store";
    public static TableUtils tableUtils = new TableUtils();

    public static void main(String[] args) throws InterruptedException, IOException {

        // Create the HBase table to store the aggregated data
        tableUtils.createRealTimeTable();

        // Set up the Spark Streaming context
        SparkConf sparkConf = new SparkConf().setAppName("Electronic store");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        // Set the log level to ERROR to reduce console output
        ssc.sparkContext().setLogLevel("ERROR");

        // Set the Kafka consumer parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka:29092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // Set the topic to subscribe to
        List<String> topics = Arrays.asList(KAFKA_TOPIC);

        // Create the Kafka stream
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Create an ObjectMapper to parse JSON data
        ObjectMapper mapper = new ObjectMapper();

        // Parse the JSON data into a stream of JsonNode objects and cache
        JavaDStream<JsonNode> datas = stream.map(record -> record.value().toString())
                .map((String line) -> mapper.readTree(line)).cache();

        // Aggregate the event counts by type (view, cart, purchase)
        JavaPairDStream<String, Integer> eventTypeCounts = datas
                .mapToPair((JsonNode actualObj) -> {
                    return new Tuple2<String, Integer>(actualObj.get("event_type").asText(), 1);
                }).reduceByKey((x, y) -> x + y);

        // Aggregate the category view counts by category
        JavaPairDStream<String, Integer> categoryViewCounts = datas
                .filter(obj -> obj.get("event_type").asText().equals("view"))
                .map((JsonNode obj) -> obj.get("category_code").asText(null))
                .filter(c -> c != null && !c.isBlank())
                .mapToPair(c -> new Tuple2<String, Integer>(c.split("\\.")[0], 1))
                .reduceByKey((x, y) -> x + y);

        // Print the category view counts to the console
        categoryViewCounts.print();

        // Print the event type counts to the console
        eventTypeCounts.print();

        // Write the event type counts result to Kafka and HBase
        eventTypeCounts.foreachRDD((rdd, time) -> {

            rdd.foreachPartition((events) -> {
                List<Tuple2<String, Integer>> result = IteratorUtils.toList(events);
                if (result.size() == 0)
                    return;
                // Write the event type counts to the Kafka topic "electronic-analytics"
                new KafkaWriter().writeEvents(result);

                // Write the event type counts to HBase with the computed time as the row key
                new HBaseWriter().writeEvents(result, time.toString());
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

}
