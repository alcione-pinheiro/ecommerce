package br.com.ecommerce;

import java.io.Closeable;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaDisptacher<T> implements Closeable{

	private final KafkaProducer<String, T> producer;

	KafkaDisptacher() {
		this.producer = new KafkaProducer<String, T>(properties());
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		return properties;
	}

	void send(String topic, String key, T value) {
		ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, key, value);
		Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println(data.topic() + "::: partition: " + data.partition() + "| offset: " + data.offset()
			+ "| timestamp: " + data.timestamp());
		};
		try {
			producer.send(record, callback).get();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	public void close() {
		producer.close();
	}

}
