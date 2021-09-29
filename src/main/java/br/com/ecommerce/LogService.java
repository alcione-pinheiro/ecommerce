package br.com.ecommerce;

import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class LogService {

	public static void main(String[] args) {
		LogService logService = new LogService();
		try (KafkaService<Object> service = new KafkaService<Object>(Pattern.compile("ECOMMERCE.*"), LogService.class.getSimpleName(), logService::parse, Object.class)) {
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, Object> record) {
		System.out.println("-----------------------LOG------------------------");
		System.out.println(record.toString());
		System.out.println("--------------------------------------------------");
	}	

}
