/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		try{
                        String query_Customer = "SELECT id FROM Customer";
                        List<List<String>> Customer_Database = esql.executeQueryAndReturnResult(query_Customer);
                        int cid = Customer_Database.size();
                        System.out.print("First Name: ");
                        String fname = in.readLine();
                        System.out.print("Last Name: ");
                        String lname = in.readLine();
                        System.out.print("Phone Number: ");
                        String phone = in.readLine();
                        System.out.print("Current Address: ");
                        String address = in.readLine();
                        String insert = "INSERT INTO Customer VALUES(" + Integer.toString(cid) + ", \'" + fname + "\', \'" + lname + "\', \'" + phone + "\', \'" + address + "\')";
                        esql.executeUpdate(insert);
                        System.out.println("New Customer Info Added");
                }
                catch(Exception E) {
                        System.err.println(E.getMessage());
                }
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		try{
                        String query_Mechanic = "SELECT id FROM Mechanic";
                        List<List<String>> Mechanic_Database = esql.executeQueryAndReturnResult(query_Mechanic);
                        int mid = Mechanic_Database.size();
                        Scanner input_Mec_info = new Scanner(System.in);
                        System.out.print("First Name: ");
                        String fname = in.readLine();
                        System.out.print("Last Name: ");
                        String lname = in.readLine();
                        System.out.print("Year Experience: ");
                        int experience = input_Mec_info.nextInt();
                        String insert = "INSERT INTO Mechanic VALUES(" + Integer.toString(mid) + ", \'" + fname + "\', \'" + lname + "\', " + Integer.toString(experience) + ")";
                        esql.executeUpdate(insert);
                        System.out.println("New Mechanic Info Added");
                }
                catch(Exception E) {
                        System.err.println(E.getMessage());
                }
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
                        Scanner input_Car_info = new Scanner(System.in);
			System.out.print("How many vehicles do you have: ");
                        int num = input_Car_info.nextInt();
			for(int i = 1; i <= num; i++)
                        {
                        	System.out.print("Vehicle ID Number: ");
                        	String vin = in.readLine();
                        	System.out.print("Vehicle Make: ");
                        	String make = in.readLine();
                        	System.out.print("Vehicle Model: ");
                        	String model = in.readLine();
                        	System.out.print("Vehicle Year: ");
                        	int year = input_Car_info.nextInt();
                        	String insert = "INSERT INTO Car VALUES(\'" + vin + "\', \'" + make + "\', \'" + model + "\', " + Integer.toString(year) + ")";
                        	esql.executeUpdate(insert);
                        	System.out.println("New Vehicle Info Added");
				String query_Owner = "SELECT ownership_id FROM Owns";
                        	List<List<String>> Owner_Database = esql.executeQueryAndReturnResult(query_Owner);
                        	int ownership_id = Owner_Database.size();
                        	Scanner input_Owner = new Scanner(System.in);
                        	System.out.print("Input the ID of owner: ");
                        	int customer_id = input_Owner.nextInt();
                        	String insert2 = "INSERT INTO Owns VALUES(" + Integer.toString(ownership_id) + ", " + Integer.toString(customer_id) + ", \'" + vin + "\')";
                        	esql.executeUpdate(insert2);
                        	System.out.println("......");
                        	System.out.println("New Ownership Info Added");
			}
                }
                catch(Exception E) {
                        System.err.println(E.getMessage());
                }
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		try{
			System.out.print("Enter your last name: ");
                        String Lname = in.readLine();
                        String Query_Customer1 = "SELECT C1.id FROM Customer C1 WHERE C1.lname = \'" + Lname + "\'";
                        List<List<String>> Customer_Database1 = esql.executeQueryAndReturnResult(Query_Customer1);
                        int numTuple = Customer_Database1.size();
                        if(numTuple > 0){
                                System.out.println("Here are the customers that match your search:");
                                String Query_Customer2 = "SELECT C2.fname FROM Customer C2 WHERE C2.lname = \'" + Lname + "\'";
                                List<List<String>> Customer_Database2 = esql.executeQueryAndReturnResult(Query_Customer2);
                                for(int i = 0; i < numTuple; i++){
                                        System.out.println(Customer_Database2.get(i).get(0) + Lname + " (ID: " + Customer_Database1.get(i).get(0) + ")");
                                }
				Scanner input_id = new Scanner(System.in);
                                System.out.print("Choose one customer by entering the given ID to search service requests: ");
                                int id = input_id.nextInt(); 
                                int match = 0;
                                for(int j = 0; j < numTuple; j++){
                                        if(id == Integer.parseInt(Customer_Database1.get(j).get(0))){
                                                match++;
                                        }
                                }
				if(match > 0){
					String Query_Owns = "SELECT O.car_vin FROM Owns O WHERE O.customer_id = \'" + id + "\'";
                                        List<List<String>> Owns_Database = esql.executeQueryAndReturnResult(Query_Owns);
                                        String Query_Service_Request1 = "SELECT SR1.rid, SR1.car_vin FROM Service_Request SR1 WHERE SR1.customer_id = \'" + id + "\'";
                                        List<List<String>> Request_Database1 = esql.executeQueryAndReturnResult(Query_Service_Request1);
                                        int numOwned = Owns_Database.size();
                                        int numTuple2 = Request_Database1.size();
                                        int numRegistered = 0;
                                        List<String> registeredVehicle = new ArrayList<String>();
					List<Integer> ServiceID = new ArrayList<Integer>();
                                        for(int a = 0; a < numOwned; a++){
                                                for(int b = 0; b < numTuple2; b++){
                                                        if(Long.parseLong(Owns_Database.get(a).get(0).substring(6)) == Long.parseLong(Request_Database1.get(b).get(1).substring(6))){
                                                                numRegistered++;
                                                                registeredVehicle.add(Request_Database1.get(b).get(1));
								ServiceID.add(Integer.parseInt(Request_Database1.get(b).get(0)));
                                                        }
                                                }
                                        }
                                        if(numRegistered > 0){
                                                System.out.println("Here are the vehicles that you registered for a service:");
                                                for(int k = 0; k < registeredVehicle.size(); k++){
                                                        System.out.println(registeredVehicle.get(k) + " (rid: " + Integer.toString(ServiceID.get(k)) + ")");
                                                }
                                        }
					else if(numRegistered == 0){
                                                System.out.println("Result not found! Request a service for your new vehicle");
                                                Scanner input_Service_Request = new Scanner(System.in);
                                                System.out.print("How many vehicles are you registering for services: ");
                                                int numVehicle = input_Service_Request.nextInt();
                                                for(int l = 1; l <= numVehicle; l++){
                                                        String Query_Service_Request2 = "SELECT SR2.rid FROM Service_Request SR2";
                                                        List<List<String>> Request_Database2 = esql.executeQueryAndReturnResult(Query_Service_Request2);
                                                        int rid = Request_Database2.size();
                                                        System.out.println("Vehicle " + l);
                                                        System.out.print("Vehicle ID Number: ");
                                                        String car_vin = in.readLine();
							int owning = 0;
                                                        for(int c = 0; c < numOwned; c++)
                                                        {
                                                                if(Long.parseLong(car_vin.substring(6)) == Long.parseLong(Owns_Database.get(c).get(0).substring(6)))
                                                                {
                                                                        owning++;
                                                                }
                                                        }
							if(owning > 0){
                                                        	System.out.print("Request Date: ");
                                                        	String date = in.readLine();
                                                        	System.out.print("Vehicle Odometer: ");
                                                        	int odometer = input_Service_Request.nextInt();
                                                        	System.out.print("Service Purpose: ");
                                                        	String complain = in.readLine();
                                                        	String insert = "INSERT INTO Service_Request VALUES(" + Integer.toString(rid) + ", " + Integer.toString(id) + ", \'" + car_vin + "\', \'" + date + "\', " + Integer.toString(odometer) + ", \'" + complain + "\')";
                                                        	esql.executeUpdate(insert);
                                                        	System.out.println("New Service Initiated!");
							}
							else if(owning == 0){
                                                                System.out.println("The VIN of your vehicle does not match our record!");
								System.out.println("Please enter the info of your vehicle");
                                				Scanner input_Car_info = new Scanner(System.in);
                                				System.out.print("Vehicle ID Number: ");
                                				String vin = in.readLine();
                                				System.out.print("Vehicle Make: ");
                                				String make = in.readLine();
                                				System.out.print("Vehicle Model: ");
                                				String model = in.readLine();
                                				System.out.print("Vehicle Year: ");
                                				int year = input_Car_info.nextInt();
                                				String insert2 = "INSERT INTO Car VALUES(\'" + vin + "\', \'" + make + "\', \'" + model + "\', " + Integer.toString(year) + ")";
                                				esql.executeUpdate(insert2);
                                				System.out.println("New Car Info Added");
								System.out.println("Recording the info of the ownership");
                                				String query_Owner = "SELECT ownership_id FROM Owns";
                                				List<List<String>> Owner_Database = esql.executeQueryAndReturnResult(query_Owner);
                                				int ownership_id = Owner_Database.size();
                                				String insert3 = "INSERT INTO Owns VALUES(" + Integer.toString(ownership_id) + ", " + Integer.toString(id) + ", \'" + vin + "\')";
                                				esql.executeUpdate(insert3);
                                				System.out.println("......");
                                				System.out.println("New Ownership Info Added");
                                                        }
                                                }
                                        }
				}
				else if(match == 0){
                                        System.out.println("ID not found! Quit back to menu");
                                }
                        }
                        else if(numTuple == 0)
                        {
                                System.out.println("Result not found! Please enter your info");
                                String query_Customer = "SELECT id FROM Customer";
                                List<List<String>> Customer_Database = esql.executeQueryAndReturnResult(query_Customer);
                                int cid = Customer_Database.size();
                                System.out.print("First Name: ");
                                String fname = in.readLine();
                                System.out.print("Last Name: ");
                                String lname = in.readLine();
                                System.out.print("Phone Number: ");
                                String phone = in.readLine();
                                System.out.print("Current Address: ");
                                String address = in.readLine();
                                String insert = "INSERT INTO Customer VALUES(" + Integer.toString(cid) + ", \'" + fname + "\', \'" + lname + "\', \'" + phone + "\', \'" + address + "\')";
                                esql.executeUpdate(insert);
                                System.out.println("New Customer Info Added");
                        }
		}
		catch(Exception E) {
                        System.err.println(E.getMessage());
                }
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		try{
			Scanner input_verify = new Scanner(System.in);
                        System.out.print("Enter your employee ID: ");
                        int Eid = input_verify.nextInt();
                        String Query_Mechanic = "SELECT M.id FROM Mechanic M";
                        List<List<String>> Mechanic_Database = esql.executeQueryAndReturnResult(Query_Mechanic);
                        int numTuple1 = Mechanic_Database.size();
                        int exist1 = 0;
                        for(int i = 0; i < numTuple1; i++){
                                if(Integer.parseInt(Mechanic_Database.get(i).get(0)) == Eid){
                                        exist1++;
                                }
                        }
                        if(exist1 > 0){
                                System.out.println("The mechanic exists!");
                        }
			else{
                                System.out.println("The mechanic does not exist!");
                        }
                        System.out.print("Enter the service request ID: ");
                        int SRid = input_verify.nextInt();
                        String Query_Service_Request1 = "SELECT SR1.rid FROM Service_Request SR1";
                        List<List<String>> Request_Database1 = esql.executeQueryAndReturnResult(Query_Service_Request1);
                        int numTuple2 = Request_Database1.size();
                        int exist2 = 0;
                        for(int j = 0; j < numTuple2; j++){
                                if(Integer.parseInt(Request_Database1.get(j).get(0)) == SRid){
                                        exist2++;
                                }
                        }
                        if(exist2 > 0){
                                System.out.println("The request exists!");
                        }
			else{
                                System.out.println("The request does not exist!");
                        }
                        String Query_Close_Request = "SELECT CR.date FROM Closed_Request CR WHERE CR.mid = \'" + Eid + "\' AND CR.rid = \'" + SRid + "\'";
                        List<List<String>> Close_Database = esql.executeQueryAndReturnResult(Query_Close_Request);
			int check = Close_Database.size();
                        if(check > 0){
				System.out.println("Verifying Closed Date......");
                                System.out.println("......");
				String Cdate = Close_Database.get(0).get(0);
				if(exist1 > 0 && exist2 > 0){
					String Query_Close_Request1 = "SELECT CR1.mid FROM Closed_Request CR1";
                        	        List<List<String>> Close_Database1 = esql.executeQueryAndReturnResult(Query_Close_Request1);
                        	        int numTuple3 = Close_Database1.size();
                        	        int match1 = 0;
                        	        for(int k = 0; k < numTuple3; k++){
                        	                if(Integer.parseInt(Close_Database1.get(k).get(0)) == Eid){
                        	                        match1++;
                        	                }
                        	        }
                        	        String Query_Close_Request2 = "SELECT CR2.rid FROM Closed_Request CR2";
                                	List<List<String>> Close_Database2 = esql.executeQueryAndReturnResult(Query_Close_Request2);
                                	int numTuple4 = Close_Database2.size();
                                	int match2 = 0;
                                	for(int l = 0; l < numTuple4; l++){
                                	        if(Integer.parseInt(Close_Database2.get(l).get(0)) == SRid){
                                	                match2++;
                                	        }
                                	}
                                	String Query_Service_Request2 = "SELECT SR2.date FROM Service_Request SR2 WHERE SR2.rid = \'" + SRid + "\'";
                                	List<List<String>> Request_Database2 = esql.executeQueryAndReturnResult(Query_Service_Request2);
                                	String Rdate = Request_Database2.get(0).get(0);
                                        int compareL1 = Integer.parseInt(Rdate.substring(0,3));//year
                                        int compareR1 = Integer.parseInt(Cdate.substring(0,3));//year
                                        int compareL2 = Integer.parseInt(Rdate.substring(5,6));//month
                                        int compareR2 = Integer.parseInt(Cdate.substring(5,6));//month
                                        int compareL3 = Integer.parseInt(Rdate.substring(8));//day
                                        int compareR3 = Integer.parseInt(Cdate.substring(8));//day
                                	if(compareL1 < compareR1 || (compareL1 == compareR1 && compareL2 < compareR2) || (compareL1 == compareR1 && compareL2 == compareR2 && compareL3 < compareR3)){
                                	        if(match1 > 0 && match2 > 0){
                                	                System.out.println("The closed request has been created!");
                                	        }
                                	}
					else{
                                	        System.out.println("Invalid closed date! Please recreate the closed record");
                                	}
				}	
			}
			else if(check == 0){
                                if(exist1 > 0 && exist2 > 0){
                                        System.out.println("Recording Checking...");
                                        System.out.println("Data does not match! Quit back to menu");
                                }
                                else{
                                        System.out.println("The closed request does not exist! Please create a new closed request");
                                        String query_Closed_Request = "SELECT wid FROM Closed_Request";
                                        List<List<String>> Closed_Request_Database = esql.executeQueryAndReturnResult(query_Closed_Request);
                                        int wid = Closed_Request_Database.size() + 1;
                                        Scanner input_Closed = new Scanner(System.in);
                                        System.out.print("Request ID: ");
                                        int rid = wid;
					System.out.println(rid);
                                        System.out.print("Mechanic ID: ");
                                        int mid = input_Closed.nextInt();
                                        System.out.print("Closed Date: ");
                                        String date = in.readLine();
                                        System.out.print("Service Comment: ");
                                        String comment = in.readLine();
                                        System.out.print("Service Bill: $");
                                        int bill = input_Closed.nextInt();
                                        String insert = "INSERT INTO Closed_Request VALUES(" + Integer.toString(wid) + ", " + Integer.toString(rid) + ", " + Integer.toString(mid) + ", \'" + date + "\', \'" + comment + "\', " + Integer.toString(bill) + ")";
                                        esql.executeUpdate(insert);
                                        System.out.println("New Closed Record Added");
                                }
                        }
		}
		catch(Exception E) {
                        System.err.println(E.getMessage());
                }
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
			String query = "SELECT C.fname, C.lname, CR.date, CR.comment, CR.bill FROM Customer AS C, Closed_Request AS CR, Service_Request AS S WHERE S.customer_id = C.id AND S.rid = CR.rid AND CR.bill < 100";
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e){
			System.out.println("Query 6 failure");
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try{
			String query = "SELECT C.fname, C.lname, COUNT(*) FROM Owns AS O INNER JOIN Customer AS C ON O.customer_id = C.id GROUP BY c.id HAVING COUNT(*) > 20";
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e){
			System.out.println("Query 7 failure");
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try{
			String query = "SELECT s.rid, S.customer_id, C.vin, C.make, C.model, C.year, S.odometer FROM Car AS C, Service_Request AS S WHERE C.vin = S.car_vin AND C.year < 1995 AND S.odometer < 50000";
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e){
			System.out.println("Query 8 failure");
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		//
		try{
			System.out.print("How many entries?: ");
			int k = Integer.parseInt(in.readLine());
			String query = "SELECT C.make, C.model, C.year, C.vin, A.total_requests FROM Car AS C, (SELECT S.car_vin, COUNT(*) AS total_requests FROM Service_Request AS S GROUP BY S.car_vin) AS A WHERE C.vin = A.car_vin ORDER BY A.total_requests DESC LIMIT " + Integer.toString(k);
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e){
			System.out.println("Query 9 failure");
			System.err.println(e.getMessage());
		}
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		//
		try{
			String query = "SELECT C.fname, C.lname, A.total_bill FROM Customer AS C, (SELECT B.customer_id, SUM(B.bill) AS total_bill FROM (SELECT S.customer_id, CR.rid, CR.bill FROM Closed_Request AS CR INNER JOIN Service_Request AS S ON s.rid = CR.rid) AS B GROUP BY B.customer_id) AS A WHERE A.customer_id = C.id ORDER BY total_bill DESC";
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e){
			System.out.println("Query 10 failure");
			System.err.println(e.getMessage());
		}
	}
	
}
