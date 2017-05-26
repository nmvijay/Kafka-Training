package com.example.kafka.consumer;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * @author vijayakumar.nm
 *
 */
public class StreamConsumer {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamConsumer");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.2.3.168:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// setting offset reset to earliest so that we can re-run the demo code
		// with the same pre-loaded data
		// Note: To re-run the demo, you need to use the offset reset tool:
		// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> source = builder.stream("StreamInTopic");

		KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
			@Override
			public Iterable<String> apply(String value) {
				System.out.println("value: " + value);
				return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
			}
		}).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				System.out.println("In Loop key: " + key + ", value: " + value);
				return new KeyValue<>("VIJAY", "KUMAR");
			}
		}).filter(new Predicate<String, String>() {

			@Override
			public boolean test(String key, String value) {
				System.out.println("in filter key: " + key + ", value:" + value);
				return false;
			}
		}).groupByKey().count("Counts");

		// need to override value serde to Long type
		counts.to(Serdes.String(), Serdes.Long(), "StreamOutTopic");

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the
		// input data is finite.
		// Thread.sleep(5000L);

		// streams.close();
	}
}