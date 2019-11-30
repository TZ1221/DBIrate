


import static org.junit.Assert.*;


import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.junit.FixMethodOrder;  
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runners.MethodSorters;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class Irate_test {
	
	
    static void parseData(PreparedStatement preparedStatement, String file, int columns) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] data = line.trim().split("\\|");
                if (data.length != columns) {
                    continue;
                }

                String timeStampPattern = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]";
                for (int i = 0; i < columns; i++) {
                    if (data[i].matches(timeStampPattern)) {
                        Timestamp ts = Timestamp.valueOf(data[i]);
                        preparedStatement.setTimestamp(i + 1, ts);
                    } else {
                        preparedStatement.setString(i + 1, data[i].trim());
                    }
                }

                try {
                    preparedStatement.executeUpdate();
                } catch (SQLException ex) {
                    System.out.println("~~~~~~OOPS~~~~~~ Invalid Record from " + file + " : " + Arrays.toString(data));
                    System.out.println("Error message: " + ex.getMessage() + "\n");
                    //ex.printStackTrace();
                }
            }
        } catch (SQLException | IOException e) {
            System.out.println("Error message: " + e.getMessage() + "\n");
            //e.printStackTrace();
        }
    }
    
    

	@Test
	public void test_0010_Message() throws IOException, AWTException {
		
		 	System.out.println("The test starts here");
		 
		 	Tables.main(null);
	
	        // the default framework is embedded
	        String protocol = "jdbc:derby:";
	        String dbName = "iRate";
	        String connStr = protocol + dbName + ";create=true";

	        // tables tested by this program
	        String dbTables[] = {
	                "Attendance", "Endorsement",       // relations
	                "Review", "Customer", "Movie"        // entities
	        };


	        Properties props = new Properties(); // connection properties
	        // providing a user name and password is optional in the embedded
	        // and derbyclient frameworks
	        props.put("user", "user1");
	        props.put("password", "user1");

	        // result set for queries
	        ResultSet rs = null;
	        try (
	                // connect to database
	                Connection conn = DriverManager.getConnection(connStr, props);
	                Statement stmt = conn.createStatement();

	                // insert prepared statements
	                PreparedStatement insertRow_Customer = conn.prepareStatement(
	                        "insert into Customer(customer_Name, email, join_date) values(?, ?, ?)");
	                PreparedStatement insertRow_Movie = conn.prepareStatement(
	                        "insert into Movie(movie_title) values (?)");
	                PreparedStatement insertRow_Review = conn.prepareStatement(
	                        "insert into Review(customer_id, movie_id, review_date, rating, review) values(?, ?, ?, ?, ?)");
	                PreparedStatement insertRow_Attendance = conn.prepareStatement(
	                        "insert into Attendance(customer_id, movie_id, attendance_date) values(?, ?, ?)");
	                PreparedStatement insertRow_Endorsement = conn.prepareStatement(
	                        "insert into Endorsement(review_id, endorser_id, endorse_date) values(?, ?, ?)");

	        ) {
	            // connect to the database using URL
	            System.out.println("Connected to database " + dbName);

	            // clear data from tables
	            for (String tbl : dbTables) {
	                try {
	                    stmt.executeUpdate("delete from " + tbl);
	                    System.out.println("Truncated table " + tbl);
	                } catch (SQLException ex) {
	                    System.out.println("Did not truncate table " + tbl);
	                }
	            }


	            System.out.println("\n-----------Start building Database From text files-------------\n");

	            parseData(insertRow_Customer, "customer_data.txt", 3);
	            System.out.println("finish inserting into table customer");
	            parseData(insertRow_Movie, "movie_data.txt", 1);
	            System.out.println("finish inserting into table Movie");
	            parseData(insertRow_Attendance, "attendance_data.txt", 3);
	            System.out.println("finish inserting into table attendance");
	            parseData(insertRow_Review, "review_data.txt", 5);
	            System.out.println("finish inserting into table review");
	            parseData(insertRow_Endorsement, "endorsement_data.txt", 3);
	            System.out.println("finish inserting into table endorsement\n");

	            
	            
		

	            // print number of rows in tables
		        	String tbl= "Attendance";
	                rs = stmt.executeQuery("select count(*) from " + tbl);
	                if (rs.next()) {
	                	int count = rs.getInt(1);
	                	System.out.printf("Table %s : count: %d\n", tbl, count);
	                	assertEquals(count,9);
	                }
	               
	                
	                

		        	tbl= "Endorsement";
	                rs = stmt.executeQuery("select count(*) from " + tbl);
	                if (rs.next()) {
	                	int count = rs.getInt(1);
	                	System.out.printf("Table %s : count: %d\n", tbl, count);
	                	assertEquals(count,7);
	                }
                

	                
	                tbl= "Review";
	                rs = stmt.executeQuery("select count(*) from " + tbl);
	                if (rs.next()) {
	                	int count = rs.getInt(1);
	                	System.out.printf("Table %s : count: %d\n", tbl, count);
	                	assertEquals(count,8);
	                }
	                

	                tbl= "Customer";
	                rs = stmt.executeQuery("select count(*) from " + tbl);
	                if (rs.next()) {
	                	int count = rs.getInt(1);
	                	System.out.printf("Table %s : count: %d\n", tbl, count);
	                	assertEquals(count,10);
	                }
	                

	                tbl= "Movie";
	                rs = stmt.executeQuery("select count(*) from " + tbl);
	                if (rs.next()) {
	                	int count = rs.getInt(1);
	                	System.out.printf("Table %s : count: %d\n", tbl, count);
	                	assertEquals(count,10);
	                }
	                rs.close();

	            
	            
	            printTable.printCustomer(conn);
	            printTable.printMovie(conn);
	            printTable.printReview(conn);
	            printTable.printAttendance(conn);
	            printTable.printEndorsement(conn);

	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
		
		
		
	}
	

	
	/**

	/**
	 * Run the tests in this class.
	 * 
	 * @param args the program arguments
	 */
	public static void main(String[] args) {
	    Result result = JUnitCore.runClasses(Irate_test.class);
	    
	    System.out.println("[Unit Test Results]");
	    System.out.println();
	    
	    if (result.getFailureCount() > 0) {
	    	System.out.println("Test failure details:");
		    for (Failure failure : result.getFailures()) {
		       System.out.println(failure.toString());
		    }
		    System.out.println();
	    }
	    
	    int passCount = result.getRunCount()-result.getFailureCount()-result.getIgnoreCount(); 
	    System.out.println("Test summary:");
	    System.out.println("* Total tests = " + result.getRunCount());
	    System.out.println("* Passed tests: " + passCount);
	    System.out.println("* Failed tests = " + result.getFailureCount());
	    System.out.println("* Inactive tests = " + result.getIgnoreCount());
	}
}
