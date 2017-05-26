package com.example.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.io.Resources;

/**
 * @author vijayakumar.nm
 *
 */
public class Consumer {

	public static List<String> topics = Arrays.asList("TopicOne");

	public static void main(String[] args) throws IOException {

		KafkaConsumer<String, String> consumer;
		try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			consumer = new KafkaConsumer<>(properties);
		}
		consumer.subscribe(topics);
		int timeouts = 0;

		System.out.println(consumer.listTopics().keySet());

		while (true) {
			// read records with a short timeout. If we time out, we don't
			// really care.
			ConsumerRecords<String, String> records = consumer.poll(200);
			if (records.count() == 0) {
				timeouts++;
				// System.out.println("timeout");
			} else {
				// System.out.printf("Got %d records after %d timeouts\n",
				// records.count(), timeouts);
				timeouts = 0;
			}

			for (ConsumerRecord<String, String> record : records) {
				long latency;
				if (record.key().startsWith("key-")) {
					// ignore
				} else if (record.key().startsWith("marker-")) {
					latency = (long) (System.nanoTime() - Long.parseLong(record.value()));
					System.out.println("offset: " + record.offset() + ", partition: " + record.partition() + ", key: "
							+ record.key() + ", latency: " + latency);
				} else {
					System.out.println("offset: " + record.offset() + ", partition: " + record.partition() + ", key: "
							+ record.key() + ", value: " + record.value());
				}
			}
		}
	}
}
