//**************************************//
//      	Jamie Corr      	   		//
//      	jccorr@calpoly.edu     		//
//     			 	              		//
//			Salonee Thanawala 	  		//
//       	sthanawa@calpoly.edu   		//
//										//
//**************************************//  

import java.sql.*;
import java.util.*;
import java.io.*;
import java.lang.*;

public class JDBCTestMysql1 {
    
    private static Connection conn;
	private static String ticker;
	static BufferedWriter writer;    		

    
    public static void main(String args[]) throws IOException {
        connectDB();
        
        try {
            writer = new BufferedWriter(new FileWriter("/Users/jamiecorr/Desktop/365/Lab8/NYSE/TESTER.html"));  
        } catch (Exception ex) {};
        
        prepareHTML("/Users/jamiecorr/Desktop/365/Lab8/NYSE/htmlBegin.html");
        generalStockData();        
        prepareHTML("/Users/jamiecorr/Desktop/365/Lab8/NYSE/htmlEnd.html");
        
        try {
            writer.close();
            conn.close();
        }
        catch (Exception ex) {
            System.out.println("Unable to close connection");
        };
    }
    
    public static void generalStockData() {
    	//	Q1
    	//	Report the total number of securities traded at the start of 2016, 
    	String query1_1 = "SELECT count(*) AS 'Traded at the start of 2016' FROM Securities WHERE (StartDate BETWEEN '2016-01-01' AND '2016-03-31')";
    	runQuery(query1_1);
    	
    	//	total number of securities traded at the end of 2016
    	String query1_2 = "SELECT count(*) AS 'Traded at the end of 2016' FROM Securities WHERE (StartDate BETWEEN '2016-10-01' AND '2016-12-31')";
    	runQuery(query1_2);

    	//  total number of securities whose prices saw increase 
    	// 	between the end of 2015 and the end of 2016
    	String query1_3 = "SELECT count(*) AS 'Price Increase' FROM Prices s1, Prices s2 WHERE (s1.Day BETWEEN '2015-10-01' AND '2015-12-31') AND (s2.Day BETWEEN '2016-10-01' AND '2016-12-31') AND (s1.Open < s2.Open)";
//    	runQuery(query1_3);
    	
    	// 	total number of securities whose prices saw decrease 
    	//	between the end of 2015 and the end of 2016.
    	String query1_4 = "SELECT count(*) AS 'Price Decrease' FROM Prices s1, Prices s2 WHERE (s1.Day BETWEEN '2015-10-01' AND '2015-12-31') AND (s2.Day BETWEEN '2016-10-01' AND '2016-12-31') AND (s1.Open > s2.Open)";
//    	runQuery(query1_4);
    	
    	//  Q2
    	//	Report the top 10 stocks that were most heavily traded in 2016
    	String query2 = "SELECT DISTINCT Ticker AS 'Most heavily traded in 2016' FROM Prices ORDER BY Volume LIMIT 10";
    	runQuery(query2);
    }
    
    public static void individualStockData(String ticker) {

    }
    
    public static int runQuery(String query) {
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
    
    public static void parseInput(String arg) {
    	//if file of list of tickers
		if (arg.length() > 4) {
			readFileByLine(arg);
		}
		//if one ticker
		else {
			individualStockData(arg);	
		}
	}
	 
	public static void readFileByLine(String fileName) {	
   	  	try {
   	  			File file = new File(fileName);
   	   			Scanner scanner = new Scanner(file);
   	   		
   	   			while(scanner.hasNext()) {
   	   				individualStockData(scanner.next());	
   	   			}
   	  			scanner.close();
   	 	 	} catch (FileNotFoundException e) {
   	  	}	
	}
	
	public static void prepareHTML(String fileName) throws IOException {	
   	  	try {
	  			File file = new File(fileName);
   	   			Scanner scanner = new Scanner(file);
	  			while(scanner.hasNext()) {
   	   				writer.append(scanner.nextLine() + "\n");
   	   			}
   	  			scanner.close();
   	 	 	} catch (FileNotFoundException e) {
   	  	}	
	}
}