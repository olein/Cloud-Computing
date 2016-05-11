package hadoop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDataLoad {
	public static void main(String[] args) throws SQLException,
			ClassNotFoundException {
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
		
		stmt.executeQuery("load data local inpath '/media/hduser/Soft/beneficiary.csv' into table e_health.beneficiary");
		stmt.executeQuery("load data local inpath '/media/hduser/Soft/FP.csv' into table family_planning_info_core");
		System.out.println("Done");
		
		ResultSet res = stmt.executeQuery("select count(*) from beneficiary");
		while (res.next()) {
			System.out.println(res.getInt(1));
		}
		
		res = stmt.executeQuery("select count(*) from family_planning_info_core");
		while (res.next()) {
			System.out.println(res.getInt(1));
		}
	}
}
