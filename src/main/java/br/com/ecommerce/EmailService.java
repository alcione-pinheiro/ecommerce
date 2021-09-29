package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author alcione.pinheiro
 */
public class EmailService {

	public static void main(String[] args) {
		EmailService emailService = new EmailService();
		try (KafkaService<Email> service = new KafkaService<Email>("ECOMMERCE_SEND_EMAIL",
				EmailService.class.getSimpleName(), emailService::parse, Email.class)) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Email> record) {
		System.out.println("-----------------------EMAIL----------------------");
		System.out.println(record.toString());
		System.out.println("--------------------------------------------------");
	}

}
