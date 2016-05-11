package hadoop;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Date;

public class TestLoadQueryTime {
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	private static final String FILE_HEADER = "Data-Size,Hive-Time";
	
	public static void main(String[] args) throws SQLException, IOException {

		long value = 0;
		FileWriter fileWriter = null;
		
		String connectionURL = "jdbc:hive://localhost:10001/e_health";
		String drivername = "org.apache.hadoop.hive.jdbc.HiveDriver";
		String username = "root";
		String password = "root";
		try {
			Class.forName(drivername);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		Connection connnection = DriverManager.getConnection(connectionURL, username,
				password);
		if (connnection != null) {
			System.out.println("Connected");
		} else {
			System.out.println("Not Connected");
		}
		
		if (connnection != null) {
			System.out.println("You made it, take control your database now!");

			fileWriter = new FileWriter("/media/hduser/Soft/HiveTest3.csv");
			fileWriter.append(FILE_HEADER.toString());
			fileWriter.append(NEW_LINE_SEPARATOR);
			
			fileWriter.append(String.valueOf(2603));
			fileWriter.append(COMMA_DELIMITER);
			fileWriter.append(String.valueOf(19770));
			fileWriter.append(NEW_LINE_SEPARATOR);

			Statement stm = connnection.createStatement();
			for (int i = 2; i <26; i++) {
				value = 0;
				stm.executeQuery("load data local inpath '/tmp/beneficiary.csv' into table e_health.beneficiary");
				stm.executeQuery("load data local inpath '/tmp/FP.csv' into table e_health.family_planning_info_core");

				ResultSet rs = stm
						.executeQuery("SELECT count(*) FROM e_health.family_planning_info_core");
				while (rs.next()) {
					value = value + rs.getInt(1);
				}
				rs = stm.executeQuery("SELECT count(*) FROM e_health.beneficiary");
				while (rs.next()) {
					value = value + rs.getInt(1);
				}

				long start = new Date().getTime();
				
				stm.executeQuery("use e_health");
				rs = stm.executeQuery("select ma.beneficiary_id, va.date_of_birth, va.husband_name, ma.ANY_FP_METHOD, ma.WHICH_FP_METHOD, "+ 
						"ma.FP_FROM_WHERE, ma.START_TIME,va.first_name bf_first_name, va.last_name bf_last_name, va.household_number "+ 
						"from beneficiary va join FAMILY_PLANNING_INFO_CORE ma on ma.beneficiary_id = va.beneficiary_bid join "+
						"users ua on ua.internal_id like '%41699800%' "+
						"where va.ss_hierarchy like '%>41699800-5586-usrs-8f1e-38c19995ef08>%' "+
						"and (va.first_name like '%san%') "+
						"and ma.beneficiary_id like '%20151%'");

				long end = new Date().getTime();

				long MySQL_time = end - start;

				fileWriter.append(String.valueOf(value));
				fileWriter.append(COMMA_DELIMITER);
				fileWriter.append(String.valueOf(MySQL_time));
				fileWriter.append(NEW_LINE_SEPARATOR);				
					
				System.out.println("Complete step :" + i);
			}
			fileWriter.flush();
			fileWriter.close();
		}

		else {
			System.out.println("Failed to make connection!");
		}
		System.out.println("Done");		
	}

}
