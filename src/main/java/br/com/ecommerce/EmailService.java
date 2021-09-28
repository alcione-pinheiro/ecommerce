package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author alcione.pinheiro
 */
public class EmailService {

	public static void main(String[] args) {
		EmailService emailService = new EmailService();
		try (KafkaService service = new KafkaService("ECOMMERCE_SEND_EMAIL", EmailService.class.getSimpleName(),
				emailService::parse)) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------------EMAIL----------------------");
		System.out.println(record.toString());
		System.out.println("--------------------------------------------------");
	}

}
