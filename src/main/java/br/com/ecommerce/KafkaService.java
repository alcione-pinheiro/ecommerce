package br.com.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService implements Closeable{
	
	private final KafkaConsumer<String, String> consumer;
	private final ConsumerFunction parse;

	KafkaService(String topic, String group, ConsumerFunction parse) {
		this.consumer = new KafkaConsumer<String, String>(properties(group));
		this.parse = parse;
		
		List<String> listTopic = new ArrayList<String>();
		listTopic.add(topic);
		consumer.subscribe(listTopic);
	}
	
	private static Properties properties(String group) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, group + UUID.randomUUID().toString());
		return properties;
	}

	void run() {
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()) {
				for (ConsumerRecord<String, String> record : records) {
					parse.consume(record);
				}
			}
		}
	}

	public void close() {
		consumer.close();
	}

}
