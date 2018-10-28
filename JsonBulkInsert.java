import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.LinkedHashSet;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Created by jermorri on 6/14/17.
 */
public class DbInsertion {
    /**
     * This class inserts data into a mysql database with the use of prepared statements and 10,000 row batches.
     */

    // JDBC driver name and database URL local disk database.
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/amazon_dump";
    static final String USER = "root";
    static final String PASSWORD = "root";

    public static void main(String[] args) {

        Connection mysqlConnection = null;
        PreparedStatement preparedStatement = null;
        try {
            Class.forName(JDBC_DRIVER);

            System.out.println("Connection to database...");
            mysqlConnection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            String query = " insert into reviewer (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, "
                    + "unixReviewTime, reviewTime) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            System.out.println("Creating sql statement...");
            preparedStatement = mysqlConnection.prepareStatement(query);
            mysqlConnection.setAutoCommit(false);
            getJsonStrings(mysqlConnection, preparedStatement);
            preparedStatement.close();
            mysqlConnection.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void getJsonStrings(Connection mySqlConnection, PreparedStatement preparedStatement) {
        LinkedHashSet<String> jsonLineSet = new LinkedHashSet<>();
        String reviewer = null;
        try {
            FileInputStream inputStream = new FileInputStream("/Users/jeremy/Documents/AmazonData/huge_file.json");
            Scanner sc = new Scanner(inputStream, "UTF-8");
            while(sc.hasNextLine()) {
                for (int i = 0; i < 10000; i++) {
                    if (sc.hasNextLine()) {
                        jsonLineSet.add(sc.nextLine());
                    } else {
                        insertData(mySqlConnection, preparedStatement, jsonLineSet);
                        System.out.println("Done!");
                        break;
                    }
                }
                insertData(mySqlConnection, preparedStatement, jsonLineSet);
            }
            sc.close();
        }catch(IOException ie) {
            ie.printStackTrace();
        }
    }

    public static void insertData(Connection mySqlConnection, PreparedStatement preparedStatement, LinkedHashSet<String> jsonLineSet) {

        for(String jsonLine : jsonLineSet) {
            try {
                System.out.println("Executing query...");
                JsonElement jsonElement = new JsonParser().parse(jsonLine);
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                String id = jsonObject.get("reviewerID").getAsString();
                String asin = jsonObject.get("asin").getAsString();
                String name = "";
                try {
                    name = jsonObject.get("reviewerName").getAsString();
                }catch (NullPointerException npe) {
                    npe.printStackTrace();
                    name = "NULL";
                }
                String helpful = jsonObject.get("helpful").getAsJsonArray().toString().trim();
                String text = jsonObject.get("reviewText").getAsString();
                String overall = jsonObject.get("overall").getAsString();
                String summary = jsonObject.get("summary").getAsString();
                String unixReviewTime = jsonObject.get("unixReviewTime").getAsString();
                String reviewTime = jsonObject.get("reviewTime").getAsString();
                // Printing for debugging purposes.
                System.out.println(id);
                System.out.println(asin);
                System.out.println(name);
                System.out.println(helpful);
                System.out.println(text);
                System.out.println(overall);
                System.out.println(summary);
                System.out.println(unixReviewTime);
                System.out.println(reviewTime);
                // Insert statement.
                preparedStatement.setString(1, id);
                preparedStatement.setString(2, asin);
                preparedStatement.setString(3, name);
                preparedStatement.setString(4, helpful);
                preparedStatement.setString(5, text);
                preparedStatement.setString(6, overall);
                preparedStatement.setString(7, summary);
                preparedStatement.setString(8, unixReviewTime);
                preparedStatement.setString(9, reviewTime);
                preparedStatement.addBatch();
            }catch(SQLException se) {
                se.printStackTrace();
            }
        }
        try {
            preparedStatement.executeBatch();
            mySqlConnection.commit();
            preparedStatement.clearBatch();
        }catch(SQLException se) {
            se.printStackTrace();
        }
    }
}
