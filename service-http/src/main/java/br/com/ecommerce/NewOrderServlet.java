package br.com.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private final KafkaDisptacher<Order> orderDispatcher = new KafkaDisptacher<Order>();
	private final KafkaDisptacher<Email> emailDispatcher = new KafkaDisptacher<Email>();

	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String orderId = UUID.randomUUID().toString();
		BigDecimal amount = new BigDecimal(req.getParameter("amount"));
		String emailAddress = req.getParameter("email");

		Order order = new Order(orderId, amount, emailAddress);
		orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order);

		String subject = "Processing order";
		String body = "We are processing your order.";
		Email email = new Email(subject, body);
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAddress, email);

		resp.getWriter().println("New order processed.");
		resp.setStatus(HttpServletResponse.SC_OK);
	}
}
