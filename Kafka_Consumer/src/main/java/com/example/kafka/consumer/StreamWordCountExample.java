/*******************************************************
 * Copyright (C) 2004-2017 Exilant Technologies PVT LTD.
 * Email at <{glimpze@exilant.com}>
 *
 * This file is part of Kafka_Consumer
 *
 * Kafka_Consumer can not be copied and/or distributed without the express
 * permission of Exilant Technologies PVT Ltd (WWW.Exilant.com)
 * 23-May-2017
 * StreamPipeDemo.java
 *
 *******************************************************/
package com.example.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * @author vijayakumar.nm
 * 
 *         <pre>
 *         1. Create Topics
 *         bin/kafka-topics.sh --create --zookeeper 10.2.3.163:2181 --replication-factor 1 --partitions 1 --topic StreamInTopic
 *         bin/kafka-topics.sh --create --zookeeper 10.2.3.163:2181 --replication-factor 1 --partitions 1 --topic StreamOutTopic
 *         2. Subscribe
 *         bin/kafka-console-consumer.sh --bootstrap-server 10.2.3.168:9092 --from-beginning --topic StreamOutTopic
 *         3. Publish
 *         bin/kafka-console-producer.sh --broker-list 10.2.3.168:9092 --topic StreamInTopic
 *         </pre>
 */
public class StreamWordCountExample {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.2.3.168:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// setting offset reset to earliest so that we can re-run the demo code
		// with the same pre-loaded data
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> inputStream = builder.stream("StreamInTopic");

		/**
		 * inputStream.foreach(new ForeachAction<String, String>() { public void
		 * apply(String key, String value) { System.out.println(key + ": " +
		 * value); } });
		 */

		final Pattern pattern = Pattern.compile(" ");

		KafkaStreams streams = new KafkaStreams(builder, props);
		KStream<?, ?> counts = inputStream.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				.map((key, value) -> new KeyValue<Object, Object>(value, value)).groupByKey().count("CountStore")
				.mapValues(value -> Long.toString(value)).toStream();
		// counts.print();

		counts.to("StreamOutTopic");

		streams.start();

	}
}
