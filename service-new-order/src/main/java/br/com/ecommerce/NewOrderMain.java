package br.com.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * @author alcione.pinheiro
 */
public class NewOrderMain {

	public static void main(String[] args) {
		for (int i = 0; i < 20; i++) {
			try (KafkaDisptacher<Order> orderDispatcher = new KafkaDisptacher<Order>()) {
				try (KafkaDisptacher<Email> emailDispatcher = new KafkaDisptacher<Email>()) {
					String orderId = UUID.randomUUID().toString();
					BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
					String emailAddress = Math.random() + "email.com";

					Order order = new Order(orderId, amount, emailAddress);
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order);

					String subject = "Processing order";
					String body = "We are processing your order.";
					Email email = new Email(subject, body);
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAddress, email);
				}
			}
		}
	}

}
