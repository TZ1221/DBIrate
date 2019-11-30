import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Functions {

    /**
     * Determines whether 'email' is a valid email address.
     *
     * @param email the email address
     * @return true if 'email' is a valid email address
     */

    public static boolean isEmail(String email) {
        return email.matches(
                "^[\\p{L}\\p{N}\\._%+-]+@[\\p{L}\\p{N}\\.\\-]+\\.[\\p{L}]{2,}$");
    }

    private static java.sql.Timestamp getCurrentTimestamp() {
        java.util.Date date = new java.util.Date();
        return new java.sql.Timestamp(date.getTime());
    }

    // tested
    public static void freeGift(Connection conn) {

        System.out.print("Amigo >>> Enter the date for free gift (format: yyyy-MM-dd) : ");
        Scanner scanner0 = new Scanner(System.in);
        String date = scanner0.nextLine();

        while (!date.matches("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])")) {
            System.out.print("Amigo >>> Invalid date format! Enter date again : ");
            scanner0 = new Scanner(System.in);
            date = scanner0.nextLine();
        }

        if (date.matches("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])")) {
            // Date format: yyyy-MM-dd
            String endTime = " 23:59:59";
            String startTime = " 00:00:00";

            Timestamp startTimeStamp = Timestamp.valueOf(date + startTime);
            Timestamp endTimeStamp = Timestamp.valueOf(date + endTime);


            // execute query

            try {
                String query0 = "select customer_Name from Customer INNER JOIN Endorsement ON " +
                        "Customer.customer_id = Endorsement.endorser_id WHERE timestamp(Endorsement.endorse_date) BETWEEN (?) AND (?)";
                PreparedStatement invoke_freeGift = conn.prepareStatement(query0);
                invoke_freeGift.setTimestamp(1, startTimeStamp);
                invoke_freeGift.setTimestamp(2, endTimeStamp);
                ResultSet rs5 = invoke_freeGift.executeQuery();

                if (!rs5.next()) {
                    System.out.printf("There is no winner of the free concession items that day !");
                    return;
                } else {
                    do {
                        //System.out.println("The winner of the free concession items are: ");
                        System.out.println("Winners of Free Gift on day " + date + ":       " + rs5.getString("customer_Name"));
                    } while (rs5.next());
                }


                rs5.close();
            } catch (SQLException ex) {
                System.out.printf("There is no winner of the free concession items that day");
                //ex.printStackTrace();
            }
        }
    }

  

    // Tested. Select the author of  top voted review written 3 days ago
    public static void freeTicket(Connection conn) {
 
        try {
            // This query will select the review_id and its count that satisfies the requirement
            String query0 = "select Endorsement.review_id, count(*) AS nor from Endorsement LEFT JOIN Review ON Endorsement.review_id = Review.review_id" +
                    " WHERE Review.review_date < timestamp({fn TIMESTAMPADD(SQL_TSI_DAY, -3, CURRENT_TIMESTAMP)}) " +
                    " GROUP BY Endorsement.review_id ORDER BY nor DESC ";

            PreparedStatement invoke_freeTicket = conn.prepareStatement(query0);
            ResultSet rs0 = invoke_freeTicket.executeQuery();
            int topReviewId = 0;
            int reviewVote = 0;
            int movieID = 0;

            if (rs0.next()) {
                topReviewId = rs0.getInt("review_id");
                reviewVote = rs0.getInt("nor");
            }

            // This query will  select the author of the top voted review, which we obtained in query0, and all other info about that review
            String query1 = "select * from Customer LEFT JOIN Review ON Customer.customer_id = Review.customer_id WHERE Review.review_id = (?)";
            PreparedStatement invoke_freeTicket1 = conn.prepareStatement(query1);
            invoke_freeTicket1.setInt(1, topReviewId);
            ResultSet rs1 = invoke_freeTicket1.executeQuery();
            if (rs1.next()) {
                movieID = rs1.getInt("movie_id");
            }

            // This query gets the movie title of the top review
            String titleQuery = "select movie_title from Movie where movie_id = (?)";
            PreparedStatement invoke_freeTicket2 = conn.prepareStatement(titleQuery);
            invoke_freeTicket2.setInt(1, movieID);
            ResultSet rs2 = invoke_freeTicket2.executeQuery();
            if (rs2.next()) {
                System.out.println(">>      The winner of  FREE TICKET is:  " + rs1.getString("customer_Name") + " !!");
                System.out.println(">>      User " + rs1.getString("customer_Name") + "'s review: \n>>      ");
                System.out.println(">>      `" + rs1.getString("review") + "`\n>>      ");
                System.out.println(">>      for movie `" + rs2.getString("movie_title") + "` has " + reviewVote + " votes, which is the top rated review written three days earlier.");
            } else {
                System.out.printf("There is no winner of free ticket for the past 3 days\n");
                return;
            }


        } catch (SQLException ex) {
            //ex.printStackTrace();
            System.out.printf("There is no winner of free ticket for the past 3 days\n");
            System.out.println("Error message: " + ex.getMessage() + "\n");

        }
    }

  
  
 


}
