package br.com.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable{
	
	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction<T> parse;

	KafkaService(String topic, String group, ConsumerFunction<T> parse, Class<T> type) {
		this.consumer = new KafkaConsumer<String, T>(properties(group, type));
		this.parse = parse;
		
		List<String> listTopic = new ArrayList<String>();
		listTopic.add(topic);
		consumer.subscribe(listTopic);
	}
	
	KafkaService(Pattern topic, String group, ConsumerFunction<T> parse, Class<T> type) {
		this.consumer = new KafkaConsumer<String, T>(properties(group, type));
		this.parse = parse;
		consumer.subscribe(topic);
	}

	private Properties properties(String group, Class<T> type) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, group + UUID.randomUUID().toString());
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		return properties;
	}

	void run() {
		while(true) {
			ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()) {
				for (ConsumerRecord<String, T> record : records) {
					parse.consume(record);
				}
			}
		}
	}

	public void close() {
		consumer.close();
	}

}
