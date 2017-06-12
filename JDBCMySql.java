//	Jamie Corr
//	jccorr@calpoly.edu 
//     		&	 
//	Salonee Thanawala 	  
//	sthanawa@calpoly.edu   	

import java.sql.*;
import java.util.*;
import java.io.*;
import java.lang.*;

public class JDBCMySql {
    
    private static Connection conn;
    
    public static void main(String args[]) throws IOException {
        connectDB();
        listTickers(args[0]);
//        listTickers("GOOG");

        try {
            conn.close();
        }
        catch (Exception ex) {
            System.out.println("Unable to close connection");
        };
    }
    
    public static void perIndivStock(String ticker) throws IOException {
    	BufferedWriter writer = null;
    	try {
    		writer = new BufferedWriter(new FileWriter(ticker + ".html"));  
        } catch (Exception ex) {};
        
        prepareHTML("htmlBegin.html", writer);
        
        generalStockData(writer);       
        individualStockData(ticker, writer);
        
        prepareHTML("htmlEnd.html", writer);
        
        try {
            writer.close();
        }
        catch (Exception ex) {
            System.out.println("Unable to close writer");
        };
    }
    
    public static void generalStockData(BufferedWriter writer) {
    	//	Q1
    	//	Report the total number of securities traded at the start of 2016, 
    	runQuery("SELECT * FROM (SELECT count(*) AS 'Traded at the start of 2016' FROM Securities WHERE (StartDate BETWEEN '2016-01-01' AND '2016-03-31')) a, (SELECT count(*) AS 'Traded at the end of 2016' FROM Securities WHERE (StartDate BETWEEN '2016-10-01' AND '2016-12-31')) b, (SELECT count(*) AS 'Whose prices saw increase between the end of 2015 and the end of 2016' FROM Prices s1, Prices s2 WHERE (s1.Day BETWEEN '2015-10-01' AND '2015-12-31') AND (s2.Day BETWEEN '2016-10-01' AND '2016-12-31') AND s1.Open < s2.Open AND s1.Ticker = s2.Ticker) c, (SELECT count(*) AS 'Whose prices saw decrease between the end of 2015 and the end of 2016' FROM Prices s1, Prices s2 WHERE (s1.Day BETWEEN '2015-10-01' AND '2015-12-31') AND (s2.Day BETWEEN '2016-10-01' AND '2016-12-31') AND s1.Open > s2.Open) d", writer);
    	
    	//  Q2
    	//	Report the top 10 stocks that were most heavily traded in 2016
    	runQuery("SELECT DISTINCT Ticker AS 'Most heavily traded in 2016' FROM Prices ORDER BY Volume LIMIT 10", writer);
    }
    
    public static void individualStockData(String ticker, BufferedWriter thisWriter) throws IOException {
    	thisWriter.write("<h1>Individual Stock Data for: " + ticker + "</h1>");
    	
    	//  Q1
    	//  Range of dates for which the pricing data is available
    	runQuery("SELECT Ticker, min(day), max(day) FROM Prices WHERE Ticker = '" + ticker + "'", thisWriter);
    }
    
    public static int runQuery(String query, BufferedWriter writer) {
		int rowCount = 0;
    	try {
		    Statement s3 = conn.createStatement();   

		     try {
			      if (s3.execute(query)) {
			        // There's a ResultSet to be had
			        ResultSet rs = s3.getResultSet();
			        writer.append("\n\n<table>\n");

			        ResultSetMetaData rsmd = rs.getMetaData();

			        int numcols = rsmd.getColumnCount();
			    
			        // Title the table with the result set's column labels
			        writer.append("<tr>");
			        for (int i = 1; i <= numcols; i++)
			        	writer.append("<th>" + rsmd.getColumnLabel(i) + "</th>");
			        writer.append("</tr>\n");

			        while(rs.next()) {
			        	writer.append("<tr>");  // start a new row
			          for(int i = 1; i <= numcols; i++) {
			        	  writer.append("<td>");  // start a new data element
			            Object obj = rs.getObject(i);
			            if (obj != null)
			            	writer.append(obj.toString());
			            else
			            	writer.append("&nbsp;");
			            writer.append("</td>");
			            }
			          writer.append("</tr>\n");
			        }

			        // End the table
			        writer.append("</table>\n\n");
			      }
			      else {
			        // There's a count to be had
			    	  writer.append("<B>Records Affected:</B> " + s3.getUpdateCount());
			      }
			    }
			    catch (SQLException e) {
			    	writer.append("</TABLE><H1>ERROR:</H1> " + e.getMessage());
			    }
			
            
           	}  catch (Exception ee) {System.out.println(ee);}
		return rowCount;

    }
    
    public static void connectDB() {
    	try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        }
        catch (Exception ex)
        {
            System.out.println("Driver not found");
            System.out.println(ex);
        };
        
        String url = "jdbc:mysql://cslvm74.csc.calpoly.edu/nyse?";

        conn = null;
        try { 
        	conn = DriverManager.getConnection(url +"user=jccorr&password=wcorr");
        }
        catch (Exception ex)
        {
            System.out.println("Could not open connection");
            System.out.println(ex);
        };
    }
	 
	public static void listTickers(String tickets) throws IOException {	
   	  	if (tickets.contains(".txt")) {
	   	  	try {	
	            File file = new File(tickets);
	            Scanner input = new Scanner(file);
	
	            while (input.hasNext()) {
	            	String ticket = input.next();
	            	perIndivStock(ticket);
	            	
	            	//TODO: remove this line before submission, its just to check which tickets were parsed
	            	System.out.println(ticket);
	            }
	            input.close();
	
	        } catch (Exception ex) {
	            ex.printStackTrace();
	        }
   	  	}
   	  	else { 
			perIndivStock(tickets);
   	  	}
	}
	
	public static void prepareHTML(String fileName, BufferedWriter thisWriter) throws IOException {	
   	  	try {
	  			File file = new File(fileName);
   	   			Scanner scanner = new Scanner(file);
	  			while(scanner.hasNext()) {
	  				thisWriter.append(scanner.nextLine() + "\n");
   	   			}
   	  			scanner.close();
   	 	 	} catch (FileNotFoundException e) {
   	  	}	
	}
}
