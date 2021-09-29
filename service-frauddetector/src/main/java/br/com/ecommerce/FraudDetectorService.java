package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author alcione.pinheiro
 */
public class FraudDetectorService {

	public static void main(String[] args) {
		FraudDetectorService fraudService = new FraudDetectorService();
		try (KafkaService<Order> service = new KafkaService<Order>("ECOMMERCE_NEW_ORDER",
				FraudDetectorService.class.getSimpleName(), fraudService::parse, Order.class)) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("-----------------FRAUD DETECTION------------------");
		System.out.println(record.toString());
		System.out.println("--------------------------------------------------");
	}

}
