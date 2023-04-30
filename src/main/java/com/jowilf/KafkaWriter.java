package com.jowilf;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import scala.Tuple2;

public class KafkaWriter {

    // Define the topic to write the output events
    public static String OUTPUT_TOPIC = "electronic-analytics";

    // Create Kafka producer configuration properties
    Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    // Write the computed event counts to the Kafka topic for real-time
    // visualization
    public void writeEvents(List<Tuple2<String, Integer>> events) {
        Producer<String, String> producer = new KafkaProducer<>(getProps());
        try {
            // Create a JSON object to hold the aggregated events
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode actualObj = mapper.createObjectNode();

            // Loop through the events and add them to the JSON object
            for (Tuple2<String, Integer> event : events) {
                actualObj.put(event._1(), event._2());
            }

            // Send the JSON object as a message to the Kafka topic
            producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, "event_type_agg", actualObj.toString()));
        } finally {
            // Flush and close the Kafka producer
            producer.flush();
            producer.close();
        }
    }
}
