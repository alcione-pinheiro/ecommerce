package br.com.ecommerce;

import java.math.BigDecimal;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author alcione.pinheiro
 */
public class FraudDetectorService {
	
	private KafkaDisptacher<Order> dispatcher = new KafkaDisptacher<Order>();

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
		
		Order order = record.value();
		if (isFraud(order)) {
			System.out.println("Order is a fraud.");
			dispatcher.send("ECOMMERCE_REJECTED_ORDER", order.getEmail(), order);
		} else {
			System.out.println("Order processed.");
			dispatcher.send("ECOMMERCE_APPROVED_ORDER", order.getEmail(), order);
		}
		
		System.out.println("--------------------------------------------------");
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal(4500)) == 1;
	}

}
