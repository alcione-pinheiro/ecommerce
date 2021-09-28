package br.com.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * @author alcione.pinheiro
 */
public class NewOrderMain {

	public static void main(String[] args) {
		try (KafkaDisptacher<Order> orderDispatcher = new KafkaDisptacher<Order>()) {
			try (KafkaDisptacher<String> emailDispatcher = new KafkaDisptacher<String>()) {
				String userId = UUID.randomUUID().toString();
				String orderId = UUID.randomUUID().toString();
				BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
				
				Order order = new Order(userId, orderId, amount);
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

				String email = "Processing order.";
				emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
			}
		}
	}

}
