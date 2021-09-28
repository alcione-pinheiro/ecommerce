package br.com.ecommerce;

import java.util.UUID;

/**
 * @author alcione.pinheiro
 */
public class NewOrderMain {

	public static void main(String[] args) {
		try (KafkaDisptacher dispatcher = new KafkaDisptacher()) {
			String key = UUID.randomUUID().toString();
			String order = "123,54678,4554";
			String email = "Processing order.";
			
			dispatcher.send("ECOMMERCE_NEW_ORDER", key, order);
			dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
		}
	}

}
