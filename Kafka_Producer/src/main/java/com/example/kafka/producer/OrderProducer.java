package com.example.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.io.Resources;

/**
 * @author vijayakumar.nm
 * 
 *         <pre>
 * bin/kafka-topics.sh --create --zookeeper <IP>:2181 --replication-factor 2 --partitions 1 --topic OrdersTopic
 *         </pre>
 */
public class OrderProducer {

	public static String topicName = "OrdersTopic";

	public static void main(String[] args) throws IOException {
		// set up the producer
		KafkaProducer<String, String> producer;
		try (InputStream props = Resources.getResource("producer.properties").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());
			properties.put("partitions.0", "USA");
			properties.put("partitions.1", "India");
			producer = new KafkaProducer<>(properties);
		}

		try {
			ProducerRecord<String, String> rec = null;
			for (int i = 0; i <= 10; i++) {
				rec = new ProducerRecord<String, String>(topicName, getCountry(), "Order-" + i);
				producer.send(rec, new Callback() {
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						System.out.print("Message sent to parition->"
								+ metadata.partition() + " stored at offset->" + metadata.offset() );
					}
				});
				System.out.println(" with key->" + rec.key());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static String getCountry() {
		String country[] = { "USA", "India" };
		int rnd = new Random().nextInt(country.length);
		return country[rnd];
	}
}
