package com.example.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

/**
 * @author vijayakumar.nm
 *
 */
public class Producer {

	public static String topic = "TopicOne";

	public static void main(String[] args) throws IOException {
		// set up the producer
		KafkaProducer<String, String> producer;
		try (InputStream props = Resources.getResource("producer.properties").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}

		try {
			for (int i = 1; i <= 100; i++) {
				producer.send(new ProducerRecord<String, String>(topic, "key-" + i, "value-" + i));

				if (i % 10 == 0) {
					producer.send(new ProducerRecord<String, String>(topic, "marker-" + i, "" + System.nanoTime()));

					producer.flush();
					System.out.println("Sent msg number " + i);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}
