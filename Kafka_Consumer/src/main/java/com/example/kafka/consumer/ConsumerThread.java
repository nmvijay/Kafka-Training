package com.example.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.google.common.io.Resources;

/**
 * @author vijayakumar.nm
 *
 */
public class ConsumerThread implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	Properties properties = new Properties();

	public ConsumerThread(int id, String groupId, List<String> topics) {
		this.id = id;
		this.topics = topics;

		try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
			properties.load(props);
			properties.put("group.id", groupId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.consumer = new KafkaConsumer<>(properties);
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(topics);

			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {

					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("key", record.key());
					data.put("value", record.value());

					System.out.println(this.id + ": " + data);
				}

			}

		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	public static void main(String[] args) throws IOException {
		ConsumerThread consumer = new ConsumerThread(1, "Group1", OrderConsumer.topics);
		consumer.run();
	}
}
