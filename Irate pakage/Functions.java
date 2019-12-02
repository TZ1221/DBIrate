import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Functions {

	
    public static boolean Validate_Email(String email) {
        String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\."+ 
                "[a-zA-Z0-9_+&*-]+)*@" + 
                "(?:[a-zA-Z0-9-]+\\.)+[a-z" + 
                "A-Z]{2,7}$"; 
		Pattern pat = Pattern.compile(emailRegex); 
		if (email == null) 
			{return false; }
		return pat.matcher(email).matches(); 
    }




    
    public static boolean isValidDate(String d) 
    { 
        String regex = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])";
        Pattern pattern = Pattern.compile(regex); 
        Matcher matcher = pattern.matcher((CharSequence)d); 
        return matcher.matches(); 
    } 
  



    public static String freeItem(Connection conn,String date) {
    	
    	String result = "no winner";
        if (isValidDate(date)) {
            Timestamp start = Timestamp.valueOf(date + " 00:00:00");
            Timestamp end = Timestamp.valueOf(date + " 23:59:59");
            try {
                String q = "select customer_Name from Customer INNER JOIN Endorsement ON " +
                        "Endorsement.endorser_id = Customer.customer_id WHERE timestamp(Endorsement.endorse_date) BETWEEN (?) AND (?)";
                PreparedStatement invoke_freeGift = conn.prepareStatement(q);
                invoke_freeGift.setTimestamp(1, start);
                invoke_freeGift.setTimestamp(2, end);
                ResultSet r = invoke_freeGift.executeQuery();

                if (!r.next()) { 
                	System.out.printf("nobody gets the free concession items \n");
                    result="no winner";
                } else {
                    do {
                        System.out.println(r.getString("customer_Name")+"  gets a free concession items");
                        result=r.getString("customer_Name");
                    } while (r.next());
                }
                r.close();
            } catch (SQLException ex) {
                System.out.printf("nobody gets the free concession items \n");
            }
        }
		return result;
    }

  

  
    public static String freeTicket(Connection conn,String date) {
    	String result="no winner";
        int review_vote = 0;
        int review_id = 0;
        int movie_id = 0;
        
        if (isValidDate(date)) {
        try {
            Timestamp startTimeStamp = Timestamp.valueOf(date + " 00:00:00");          
            Timestamp later = new Timestamp(startTimeStamp.getTime() + (24*1000 * 60 * 60 * 3));
 
            String q = "select Endorsement.review_id, count(*) AS nor from Endorsement LEFT JOIN Review ON Endorsement.review_id = Review.review_id" +
                    " WHERE Review.review_date BETWEEN (?) AND (?) " +
                    " GROUP BY Endorsement.review_id ORDER BY nor DESC ";

            PreparedStatement invoke_freeTicket = conn.prepareStatement(q);
            invoke_freeTicket.setTimestamp(1, startTimeStamp);
            invoke_freeTicket.setTimestamp(2,later);
            ResultSet r = invoke_freeTicket.executeQuery();

            if (r.next()) {
                review_id = r.getInt("review_id");
                review_vote = r.getInt("nor");
            }

            // This query will  select the author of the top voted review, which we obtained in query0, and all other info about that review
            String q2 = "select * from Customer LEFT JOIN Review ON Customer.customer_id = Review.customer_id WHERE Review.review_id = (?)";
            PreparedStatement invoke_freeTicket1 = conn.prepareStatement(q2);
            invoke_freeTicket1.setInt(1, review_id);
            ResultSet rs1 = invoke_freeTicket1.executeQuery();
            if (rs1.next()) {
                movie_id = rs1.getInt("movie_id");
            }

      
            String q3 = "select movie_title from Movie where movie_id = (?)";
            PreparedStatement invoke_freeTicket2 = conn.prepareStatement(q3);
            invoke_freeTicket2.setInt(1, movie_id);
            ResultSet rs2 = invoke_freeTicket2.executeQuery();
            if (rs2.next()) {
                System.out.println( rs1.getString("customer_Name") + " gets a free ticket\n");
                System.out.println("Winning Review is : " + rs1.getString("review") + "\n");
                System.out.println("Votes: " + review_vote + "\n");
                result=rs1.getString("customer_Name");
            } else {
                System.out.printf("No body gets a free ticket\n");
                result="no winner";
           
            }
        } catch (SQLException ex) {
            System.out.printf("No body gets a free ticket\n");

        }
        }
		return result;
    }

  
  
 


}
