package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.TimeZone;

public class TransactionDemo {

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata")
        );
        String url = "jdbc:postgresql://localhost:5433/orders";
        String username = "postgres";
        String password = "postgres";


        Connection connection =
                DriverManager.getConnection(url, username, password);

        try {
            connection.setAutoCommit(false); //turning off autocmmit so simulate and outbox pattern

            // 1. Business operation
            String updateAccount =
                    "UPDATE accounts " +
                            "SET balance = balance + ? " +
                            "WHERE id = ?";

            PreparedStatement accountStatement =
                    connection.prepareStatement(updateAccount);

            accountStatement.setLong(1, 1000);
            accountStatement.setString(2, "user-1");

            accountStatement.executeUpdate(); //execute postgres business query

            // 2. Write event to Outbox
            String insertOutbox =
                    "INSERT INTO outbox " +
                            "(event_id, event_type, payload, status) " +
                            "VALUES (?, ?, ?, ?)";

            PreparedStatement outboxStatement =
                    connection.prepareStatement(insertOutbox);

            outboxStatement.setString(1, "TXN-001");
            outboxStatement.setString(2, "MoneyDeposited");
            outboxStatement.setString(3, "{\"account\":\"user-1\",\"amount\":1000}");
            outboxStatement.setString(4, "PENDING");

            outboxStatement.executeUpdate(); // run kafka intent query

            // 3. Commit BOTH operations
            connection.commit();

            System.out.println("TRANSACTION COMMITTED");

        } catch (Exception e) {

            connection.rollback();

            System.out.println("TRANSACTION ROLLED BACK");

            e.printStackTrace();

        } finally {
            connection.close();
        }
    }
}