package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author alcione.pinheiro
 */
public class FraudDetectorService {

	public static void main(String[] args) {
		FraudDetectorService fraudService = new FraudDetectorService();
		try (KafkaService service = new KafkaService("ECOMMERCE_NEW_ORDER", FraudDetectorService.class.getSimpleName(),
				fraudService::parse)) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------FRAUD DETECTION------------------");
		System.out.println(record.toString());
		System.out.println("--------------------------------------------------");
	}

}
