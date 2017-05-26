package com.example.kafka.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * @author vijayakumar.nm
 * 
 *         <pre>
 *         1. Create Topics
 *         bin/kafka-topics.sh --create --zookeeper <IP>:2181 --replication-factor 1 --partitions 1 --topic StreamInTopic
 *         bin/kafka-topics.sh --create --zookeeper <IP>:2181 --replication-factor 1 --partitions 1 --topic StreamOutTopic
 *         2. Subscribe
 *         bin/kafka-console-consumer.sh --bootstrap-server <IP>:9092 --from-beginning --topic StreamOutTopic
 *         3. Publish
 *         bin/kafka-console-producer.sh --broker-list <IP>:9092 --topic StreamInTopic
 *         </pre>
 */
public class StreamPipeDemo {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "<IP>:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// setting offset reset to earliest so that we can re-run the demo code
		// with the same pre-loaded data
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		KStreamBuilder builder = new KStreamBuilder();

		builder.stream("StreamInTopic").to("StreamOutTopic");

		KafkaStreams streams = new KafkaStreams(builder, props);
		// while (true) {
		streams.start();
		// Thread.sleep(5000L);
		// }
		// System.out.println("Done streaming");

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the
		// input data is finite.
		// Thread.sleep(5000L);

		// streams.close();
	}
}
