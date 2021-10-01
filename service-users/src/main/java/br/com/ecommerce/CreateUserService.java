package br.com.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {

	private Connection connection;

	public CreateUserService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		try {
		connection.createStatement()
				.execute("create table users (" + "uuid varchar(200) primary key, " + "email varchar(200))");
		} catch (SQLException e) {
			System.out.println("Table already exists.");
		}
	}

	public static void main(String[] args) throws SQLException {
		CreateUserService userService = new CreateUserService();
		try (KafkaService<Order> service = new KafkaService<Order>("ECOMMERCE_NEW_ORDER",
				CreateUserService.class.getSimpleName(), userService::parse, Order.class)) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("-----------------CREATE USER------------------");
		System.out.println(record.toString());

		Order order = record.value();
		try {
			if (!exists(order.getEmail())) {
				insertNewUser(order.getUserId(), order.getEmail());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("--------------------------------------------------");
	}

	private void insertNewUser(String userId, String email) throws SQLException {
		PreparedStatement statement = connection.prepareStatement("insert into users (uuid, email) values(?, ?)");
		statement.setString(1, userId);
		statement.setString(2, email);
		statement.execute();
		System.out.println("User added!");
	}

	private boolean exists(String email) throws SQLException {
		PreparedStatement statement = connection.prepareStatement("select uuid from users where email = ? limit 1");
		statement.setString(1, email);
		ResultSet result = statement.executeQuery();
		return result.next();
	}

}
