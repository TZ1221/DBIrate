import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Tables {

    static void createTables() {
        // the default framework is embedded
        String protocol = "jdbc:derby:";
        String dbName = "iRate";
        String connStr = protocol + dbName + ";create=true";

        // tables created by this program
        String dbTables[] = {
                "Attendance", 
                "Endorsement",       
                "Review", 
                "Customer", 
                "Movie"  
        };

        // triggers created by this program
        String dbTriggers[] = {
        		"review_limit_by_attendance", 
        		"review_limit_by_date", 
        		"review_limit_by_date2",
                "endorse_limit_by_date", 
                "endorse_limit_by_customer", 
                "endorse_limit_by_oneDay"
        };

        // procedures created by this program
        String storedFunctions[] = {
        		"Validate_Email", 
        		"freeGift", 
        		"freeTicket"
        };

        Properties props = new Properties(); // connection props
        props.put("user", "user1");
        props.put("password", "user1");

        try (
        		// connect DB
				Connection conn = DriverManager.getConnection(connStr, props);
				// statement
				Statement stmt = conn.createStatement();
        ) {
            System.out.println("Connected to and created database " + dbName);

            // drop triggers
 			for (String t : dbTriggers) {
 				try {
 					stmt.executeUpdate("drop trigger " + t);
 					System.out.println("Dropped trigger " + t);
 				} catch (SQLException ex) {
 					System.out.println("Did not drop trigger " + t);
 				}
 			}

 			// drop tables
			for (String e : dbTables) {
				try {
					stmt.executeUpdate("drop table " + e);
					System.out.println("Dropped table " + e);
				} catch (SQLException ex) {
					System.out.println(ex);
					System.out.println("Did not drop table " + e);
				}
			}

			// drop functions
			for (String f : storedFunctions) {
				try {
					stmt.executeUpdate("drop function " + f);
					System.out.println("Dropped function " + f);
				} catch (SQLException ex) {
					System.out.println("Did not drop function " + f);
				}
			}

			// function 1
			// create the Validate_Email function
            String create_Validate_Email = 
            		"CREATE function Validate_Email (Email varchar(64))"
                    + " RETURNS BOOLEAN"
                    + " PARAMETER STYLE JAVA"
                    + " LANGUAGE JAVA"
                    + " DETERMINISTIC"
                    + " NO SQL"
                    + " EXTERNAL NAME 'Functions.Validate_Email'";
            stmt.executeUpdate(create_Validate_Email);
            System.out.println("Created function Validate_Email()");


            // function 2
            // create the freeGift function
            String create_freeGift = 
            		"CREATE function freeGift (date TIMESTAMP)"
            		+ " RETURNS VARCHAR(64)"
                    + " PARAMETER STYLE JAVA"
                    + " LANGUAGE JAVA"
                    + " DETERMINISTIC"
                    + " NO SQL"
                    + " EXTERNAL NAME 'Functions.freeGift'";
            stmt.executeUpdate(create_freeGift);
            System.out.println("Created function freeGift()");

            // function 3
            // create the freeTicket
            String create_freeTicket = 
            		"CREATE function freeTicket (date TIMESTAMP)"
            		+ " RETURNS VARCHAR(64)"
                    + " PARAMETER STYLE JAVA"
                    + " LANGUAGE JAVA"
                    + " DETERMINISTIC"
                    + " NO SQL"
                    + " EXTERNAL NAME 'Functions.freeTicket'";
            stmt.executeUpdate(create_freeTicket);
            System.out.println("Created function freeTicket()");


            // create Customer table
            String createTable_Customer =
                    "CREATE table Customer ("
                    + " customer_id int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1000, INCREMENT BY 1),"
                    + " customer_Name varchar(64) NOT NULL,"
                    + " email varchar(64) NOT NULL,"
                    + " join_date TIMESTAMP NOT NULL,"
                    + " PRIMARY KEY (customer_id),"
                    + " check (Validate_Email(email))"
                    + ")";
            stmt.executeUpdate(createTable_Customer);
            System.out.println("Created table Customer");


            // create Movie table
            String createTable_Movie =
                    "CREATE table Movie ("
                    + " movie_title varchar(64) NOT NULL,"
                    + " movie_id int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000, INCREMENT BY 1),"
                    + " PRIMARY KEY (movie_id)"
                    + ")";
            stmt.executeUpdate(createTable_Movie);
            System.out.println("Created table Movie");


            // create the Review table
            String createTable_Review =
                    "CREATE table Review ("
                    + " review_id int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 100000, INCREMENT BY 1),"
                    + " customer_id int NOT NULL,"
                    + " movie_id int NOT NULL,"
                    + " review_date TIMESTAMP NOT NULL,"
                    + " rating INT NOT NULL,"
                    + " review varchar(1000) NOT NULL,"
                    + " check (rating between 0 and 5),"
                    + " UNIQUE (movie_id, customer_id)," 
                    + " PRIMARY KEY (review_id),"
                    + " FOREIGN KEY (customer_id) references Customer(customer_id) ON DELETE CASCADE,"  // if a customer is deleted, all of his or her reviews and endorsement are deleted.
                    + " FOREIGN KEY (movie_id) references Movie(movie_id) ON DELETE CASCADE"   // if a movie is deleted, all of its reviews are also deleted.
                    + ")";

            stmt.executeUpdate(createTable_Review);
            System.out.println("Created table Review");

            // create Attendance table
            String createTable_Attendance =
                    "CREATE table Attendance ("
                    + " movie_id INT NOT NULL,"
                    + " customer_id INT NOT NULL,"
                    + " attendance_date TIMESTAMP NOT NULL,"
                    + " PRIMARY KEY (movie_id, customer_id, attendance_date),"
                    + " FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) on delete cascade ,"
                    + " FOREIGN KEY (customer_id) REFERENCES Customer (customer_id) on delete cascade "
                    + " )";
            ;
            stmt.executeUpdate(createTable_Attendance);
            System.out.println("Created table Attendance");

            // create the Endorsement table
            String createTable_Endorsement =
                    "CREATE table Endorsement ("
                    + " review_id INT NOT NULL,"
                    + " customer_id INT NOT NULL,"
                    + " endorse_date TIMESTAMP NOT NULL,"
                    + " PRIMARY KEY (review_id, customer_id, endorse_date),"
                    + " FOREIGN KEY (review_id) REFERENCES Review (review_id) on delete cascade ,"
                    + " FOREIGN KEY (customer_id) REFERENCES Customer (customer_id) on delete cascade "
                    + ")";
            stmt.executeUpdate(createTable_Endorsement);
            System.out.println("Created table Endorsement");


            // trigger 1
            // Customer has to attend to the movie before review it
            String createTrigger_review_limit_by_attendance =
                    "CREATE trigger review_limit_by_attendance"
                    + " AFTER insert ON Review FOR EACH statement"
                    + " DELETE from Review where customer_id not in"
                    + " (SELECT customer_id from Attendance)";
            stmt.executeUpdate(createTrigger_review_limit_by_attendance);
            System.out.println("Created review_limit trigger for Review by Attendance");


            // trigger 2
 			// prevent review earlier than the attendance date
            String createTrigger_review_limit_by_date =
                    "CREATE trigger review_limit_by_date"
                    + " AFTER insert ON Review"
                    + " REFERENCING new as insertedRow"
                    + " FOR EACH ROW MODE DB2SQL"
                    + " DELETE from Review where  review_id = insertedRow.review_id "
                    + " AND timestamp(review_date) <"
                    + " (SELECT timestamp(attendance_date) FROM Attendance"
                    + "	WHERE Attendance.customer_id = insertedRow.customer_id"
                    + " AND Attendance.movie_id = insertedRow.movie_id)";
            stmt.executeUpdate(createTrigger_review_limit_by_date);
            System.out.println("Created review_limit trigger for Review by Date");

            // trigger 3
         	// review happens within 7 days of attendance
            String createTrigger_review_limit_by_date2 =
                    "CREATE trigger review_limit_by_date2"
                    + " AFTER insert ON Review"
                    + " REFERENCING new as insertedRow"
                    + " FOR EACH ROW MODE DB2SQL"
                    + " DELETE from Review where review_id = insertedRow.review_id "
                    + " AND (SELECT TIMESTAMP({fn TIMESTAMPADD(SQL_TSI_DAY, -7, insertedRow.review_date)})"
                    + " FROM sysibm.sysdummy1) > (SELECT timestamp(attendance_date) from Attendance"
                    + " WHERE Attendance.customer_id = insertedRow.customer_id"
                    + " AND Attendance.movie_id = insertedRow.movie_id)";
            stmt.executeUpdate(createTrigger_review_limit_by_date2);
            System.out.println("Created review_limit trigger for Review by Date2");


            // trigger4
 			// prevent endorsement_date earlier than review date
            String createTrigger_endorse_limit_by_date =
                    " CREATE trigger endorse_limit_by_date"
                    + " AFTER insert ON Endorsement"
                    + " REFERENCING new as insertedRow"
                    + " FOR EACH ROW MODE DB2SQL"
                    + " DELETE from Endorsement WHERE review_id = insertedRow.review_id"
                    + " AND customer_id = insertedRow.customer_id"
                    + " AND endorse_date = insertedRow.endorse_date"
                    + " AND timestamp(insertedRow.endorse_date) < (select timestamp(review_date)"
                    + " FROM Review WHERE Review.review_id = insertedRow.review_id)";
            stmt.executeUpdate(createTrigger_endorse_limit_by_date);
            System.out.println("Created endorse_limit trigger for endorse limit by Date");


            // trigger5
 			// prevent endorse their own review
            String createTrigger_endorse_limit_by_customer =
                    "CREATE trigger endorse_limit_by_customer"
                    + " AFTER insert ON Endorsement"
                    + " REFERENCING new as insertedRow"
                    + " FOR EACH ROW MODE DB2SQL"
                    + " DELETE from Endorsement where review_id = insertedRow.review_id"
                    + " AND customer_id = insertedRow.customer_id"
                    + " AND endorse_date = insertedRow.endorse_date"
                    + " AND review_id = (SELECT review_id from Review"
                    + " WHERE Review.customer_id = insertedRow.customer_id"
                    + " AND insertedRow.review_id = Review.review_id)";
            stmt.executeUpdate(createTrigger_endorse_limit_by_customer);
            System.out.println("Created review_limit trigger for Review by Customer");

            
            // trigger6
 			// make sure that customers can only endorse once everyday
            String createTrigger_endorse_limit_by_oneDay =
            		"CREATE trigger endorse_limit_by_oneDay"
					+ " AFTER insert ON Endorsement"
					+ " REFERENCING new as insertedRow"
					+ " FOR EACH ROW MODE DB2SQL"
					+ " DELETE from Endorsement WHERE review_id = insertedRow.review_id"
					+ " AND customer_id = insertedRow.customer_id"
					+ " AND endorse_date = insertedRow.endorse_date"
					+ " AND {fn TIMESTAMPDIFF(SQL_TSI_DAY, TIMESTAMP(insertedRow.endorse_date), (select max(TIMESTAMP(endorse_date)) as mostRecentDate"
					+ " FROM Endorsement LEFT JOIN Review ON Endorsement.review_id = Review.review_id"
					+ " WHERE TIMESTAMP(Endorsement.endorse_date) < TIMESTAMP(insertedRow.endorse_date)"
					+ " AND Endorsement.customer_id = insertedRow.customer_id"
					+ " AND Review.movie_id = (select movie_id from Review WHERE review_id = insertedRow.review_id)) )} = 0";
            stmt.executeUpdate(createTrigger_endorse_limit_by_oneDay);
            System.out.println("Created review_limit trigger for Review by oneDay");


        } catch (SQLException e) {
            System.out.println("Error message: " + e.getMessage() + "\n");
        }
    }


    public static void main(String[] args) {
    	// main function
    	System.out.println("Starting the project...");
    	createTables();
		System.out.println("Closing the project..");
    }
}