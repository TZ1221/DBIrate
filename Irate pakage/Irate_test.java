import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import java.awt.AWTException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


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
                }
            }
            br.close();
        } catch (SQLException | IOException e) {
            System.out.println("Error message: " + e.getMessage() + "\n");
        }
    }
    
    

	@Test
	public void test() throws IOException, AWTException {
		
		 	System.out.println("The test starts here");
		 	Tables.main(null);

	        String protocol = "jdbc:derby:";
	        String dbName = "iRate";
	        String connStr = protocol + dbName + ";create=true";

	        String dbTables[] = {
	                "Attendance", "Endorsement","Review", "Customer", "Movie"       
	        };

	        Properties props = new Properties(); 
	        props.put("user", "user1");
	        props.put("password", "user1");
	        ResultSet rs = null;
	        try (
	                Connection conn = DriverManager.getConnection(connStr, props);
	                Statement stmt = conn.createStatement();
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
	            System.out.println("Connected to " + dbName);

	            for (String table : dbTables) {
	                try {
	                    stmt.executeUpdate("delete from " +  table);
	                    System.out.println("deleted " +  table + "table");
	                } catch (SQLException ex) {
	                    System.out.println("Did not delete" +  table);
	                }
	            }


	            parseData(insertRow_Customer, "customer_data.txt", 3);
	            parseData(insertRow_Movie, "movie_data.txt", 1);
	            parseData(insertRow_Attendance, "attendance_data.txt", 3);
	            parseData(insertRow_Review, "review_data.txt", 5);
	            parseData(insertRow_Endorsement, "endorsement_data.txt", 3);
	            System.out.println("\n------------------------\n");
		
	            
	        	String table= "Attendance";
                rs = stmt.executeQuery("select count(*) from " + table);
                if (rs.next()) {
                	int size = rs.getInt(1);
                	System.out.printf("%s size is %d\n", table, size);
                	assertEquals(size,9);
                }
                
                table= "Customer";
                rs = stmt.executeQuery("select count(*) from " + table);
                if (rs.next()) {
                	int size = rs.getInt(1);
                	System.out.printf("%s size is %d\n", table, size);
                	assertEquals(size,10);
                }
                
                table= "Movie";
                rs = stmt.executeQuery("select count(*) from " + table);
                if (rs.next()) {
                	int size = rs.getInt(1);
                	System.out.printf("%s size is %d\n", table, size);
                	assertEquals(size,10);
                }
                rs.close();
	  

                table= "Review";
                rs = stmt.executeQuery("select count(*) from " + table);
                if (rs.next()) {
                	int size = rs.getInt(1);
                	System.out.printf("%s size is %d\n", table, size);
                	assertEquals(size,8);
                }
                

	        	table= "Endorsement";
                rs = stmt.executeQuery("select count(*) from " + table);
                if (rs.next()) {
                	int size = rs.getInt(1);
                	System.out.printf("%s size is %d\n", table, size);
                	assertEquals(size,8);
                }  

	                
	


	                



	            
	            
	            
	            System.out.println("\n------------------------\n");
				System.out.printf("Customers \n");
				
				ResultSet tc = stmt.executeQuery("select * from Customer");
				while (tc.next()) {
					int customer_id = tc.getInt(1);
					String customer_name = tc.getString(2);
					String email = tc.getString(3);
					Timestamp date = tc.getTimestamp(4);
					
					if(customer_id==1000 ) {
						assertEquals(customer_name,"Aaron");
						assertEquals(email,"Aaron@gmail.com");
					}
					
					if(customer_id==1001 ) {
						assertEquals(customer_name,"Paul");
						assertEquals(email,"Paul@gmail.com");
					}
					
					System.out.printf("Id: %d Name: %s Email: %s Date: %s\n", customer_id, customer_name, email,date);
	
				}
				tc.close();
	            
	            
		        System.out.println("\n------------------------\n");
				System.out.printf("Movies \n");
				ResultSet tm = stmt.executeQuery("select * from Movie");
				while (tm.next()) {
					String movie_title = tm.getString(1);
					int movie_id = tm.getInt(2);
					
					if(movie_id==10000 ) {
						assertEquals(movie_title,"Taxi Driver");
					}
					
					if(movie_id==10008 ) {
						assertEquals(movie_title,"Once Upon a Time In Hollywood");
					}
					
					
					System.out.printf("Title is %s id = %s \n", movie_title, movie_id);

				}
				tm.close();
	            
			    System.out.println("\n------------------------\n");
				System.out.printf("Reviews \n");
				ResultSet tr = stmt.executeQuery("select * from Review");
				while (tr.next()) {
					int review_id = tr.getInt(1);
					int customer_id = tr.getInt(2);
					int movie_id = tr.getInt(3);
					Timestamp review_date = tr.getTimestamp(4);
					int rating = tr.getInt(5);
					String review = tr.getString(6);
					
					if(review_id ==100003 ) {
						assertEquals(customer_id,1004);
						assertEquals(movie_id,10004);
						assertEquals(rating,3);
						assertEquals(review,"too bad");
						
					}
					
					
					System.out.printf("Review id is %d Movie id is %s Customer id is %s Review content is %s Date is %s Rating is %s\n", review_id, movie_id,customer_id,review, review_date, rating);
	

				}
				tr.close();
				
			    System.out.println("\n------------------------\n");
				System.out.printf("Attendances \n");
				ResultSet ta = stmt.executeQuery("select * from Attendance");
				while (ta.next()) {
					int movie_id = ta.getInt(1);
					int customer_id = ta.getInt(2);
					Timestamp attendance_date = ta.getTimestamp(3);
					
					if(movie_id ==10005 ) {
						assertEquals(customer_id,1005);	
					}
					
					System.out.printf("Movie id is %d attendance date is %s Customer id is %s \n", movie_id,  attendance_date,customer_id);
		
				}
				ta.close();
				
			    System.out.println("\n------------------------\n");
				System.out.printf("Endorsements \n");
				ResultSet te = stmt.executeQuery("select * from Endorsement");
				while (te.next()) {
					int review_id = te.getInt(1);
					int endorser_id = te.getInt(2);
					Timestamp endorse_date = te.getTimestamp(3);
					
					if(review_id ==100006 ) {
						assertEquals(endorser_id,1006);		
					}
					
					System.out.printf("Review id is %d Endorser id is: %s endorsement date is : %s\n", review_id, endorser_id, endorse_date);
			
				}
				te.close();
	            
	            
	            
	            
	            String date="2019-02-26";
	            String Giftwinner=Functions.freeGift(conn,date);
	            assertEquals(Giftwinner,"Brad");
	            
	            
	            String date2="2019-01-24";
	            String Giftwinner2=Functions.freeGift(conn,date2);
	            assertEquals(Giftwinner2,"Roberts");
	            
	            
	            String date3="2015-01-31";
	            String Giftwinner3=Functions.freeGift(conn,date3);
	            assertEquals(Giftwinner3,"no winner");
	            
	                      
	            String date4="2018-12-04";
	            String Ticketwinner4=Functions.freeTicket(conn,date4);
	            assertEquals(Ticketwinner4,"Roberts");
	            
	            
	            String date5="2014-12-04";
	            String Ticketwinner5=Functions.freeTicket(conn,date5);
	            assertEquals(Ticketwinner5,"no winner");
	            
	            String date6="2019-2-26";
	            String Ticketwinner6=Functions.freeTicket(conn,date6);
	            assertEquals(Ticketwinner6,"no winner");
	            

	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	        
	        

		
	}
	

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
