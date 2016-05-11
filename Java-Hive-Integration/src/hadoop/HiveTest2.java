package hadoop;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Date;

public class HiveTest2 {
	public static void main(String[] args) throws SQLException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
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

		Connection con = DriverManager.getConnection(connectionURL, username,
				password);
		if (con != null) {
			System.out.println("Connected");
		} else {
			System.out.println("Not Connected");
		}
		Statement stmt = con.createStatement();
		stmt.executeQuery("use e_health");
		long start = new Date().getTime();	
		
		ResultSet res = stmt.executeQuery("select ma.beneficiary_id, va.date_of_birth, va.husband_name, "+
		"ma.ANY_FP_METHOD, ma.WHICH_FP_METHOD, ma.FP_FROM_WHERE, ma.START_TIME,va.first_name bf_first_name, va.last_name bf_last_name, "+
		" va.household_number from beneficiary va join FAMILY_PLANNING_INFO_CORE ma on ma.beneficiary_id = va.beneficiary_bid "+
		"where va.ss_hierarchy like '%>41699800-5586-usrs-8f1e-38c19995ef08>%' " +
		"and (va.first_name like '%s%' or va.last_name like '%r%')");
		
		long end = new Date().getTime();
		
//		while (res.next()) {
//			//System.out.println("Query successful 01");
//			System.out.println(res.getInt(1) + " ");
//		}
		System.out.println("Query successful in time : " + (end-start));
		con.close();
	}
}