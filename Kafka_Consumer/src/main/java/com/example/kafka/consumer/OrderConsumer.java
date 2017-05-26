package com.example.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author vijayakumar.nm
 *
 */
public class OrderConsumer {

	public static List<String> topics = Arrays.asList("OrdersTopic");
	static String groupId = "OrdersGroup";

	public static void main(String[] args) {
		int numConsumers = 3;
		
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<ConsumerThread> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			ConsumerThread consumer = new ConsumerThread(i, groupId, topics);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerThread consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
}
