package org.example;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseTest {

    public static void main(String[] args) throws Exception {

        String url = "jdbc:postgresql://localhost:5432/orders";
        String username = "postgres";
        String password = "postgres";

        Connection connection =
                DriverManager.getConnection(url, username, password);

        System.out.println("Connected to PostgreSQL!");

        connection.close();
    }
}
