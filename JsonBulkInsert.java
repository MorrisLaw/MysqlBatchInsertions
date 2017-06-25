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

    // Database credentials.
    static final String USER = "root";
    static final String PASSWORD = "root";

    public static void main(String[] args) {

        Connection mysqlConnection = null;
        PreparedStatement preparedStatement = null;
        try {
            // Register the JDBC driver.
            Class.forName(JDBC_DRIVER);

            // Open a connection.
            System.out.println("Connection to database...");
            mysqlConnection = DriverManager.getConnection(DB_URL, USER, PASSWORD);

            // Create SQL query statement.
            String query = " insert into reviewer (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, "
                    + "unixReviewTime, reviewTime) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // Create prepared statement.
            System.out.println("Creating sql statement...");
            preparedStatement = mysqlConnection.prepareStatement(query);

            // Set auto-commit to false.
            mysqlConnection.setAutoCommit(false);

            // Execute the prepared statement in batches. Then insert, a batch at a time, into the database.
            getJsonStrings(mysqlConnection, preparedStatement);

            // Close up resources.
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
            // Read in the file from local disk.
            FileInputStream inputStream = new FileInputStream("/Users/jeremy/Documents/AmazonData/huge_file.json");
            // Scan each line of the json file and pass into gsonParser.
            Scanner sc = new Scanner(inputStream, "UTF-8");
            while(sc.hasNextLine()) {
                for (int i = 0; i < 10000; i++) {
                    // Next line in scanned json file.
                    if (sc.hasNextLine()) {
                        reviewer = sc.nextLine();
                        jsonLineSet.add(reviewer);
                    } else {
                        System.out.println("Done!");
                        insertData(mySqlConnection, preparedStatement, jsonLineSet);
                        break;
                    }
                }
                insertData(mySqlConnection, preparedStatement, jsonLineSet);
            }
        }catch(IOException ie) {
            ie.printStackTrace();
        }
    }

    public static void insertData(Connection mySqlConnection, PreparedStatement preparedStatement, LinkedHashSet<String> jsonLineSet) {

        for(String jsonLine : jsonLineSet) {
            try {
                System.out.println("Executing query...");
                // Parse line from json file.
                JsonElement jsonElement = new JsonParser().parse(jsonLine);
                // Retrieve as a json object.
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                // Get json object fields as string.
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
                // Add insert query to batch.
                preparedStatement.addBatch();
            }catch(SQLException se) {
                se.printStackTrace();
            }
        }
        try {
            // Execute queries within the batch.
            preparedStatement.executeBatch();
            // Commit the execution of queries.
            mySqlConnection.commit();
            // Clear and prepare batch for next round query pooling.
            preparedStatement.clearBatch();
        }catch(SQLException se) {
            se.printStackTrace();
        }
    }
}
