package hadoop;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MySQLTest {
	private static String driverName = "com.mysql.jdbc.Driver";

	public static void main(String[] args) throws SQLException,
			ClassNotFoundException {
		// Register driver and create driver instance

		Class.forName(driverName);
		// get connection

		Connection con = DriverManager.getConnection(
				"jdbc:mysql://localhost/e_health", "root", "root");
		Statement stmt = con.createStatement();
		long start = new Date().getTime();
		ResultSet res = stmt.executeQuery("select count(*) from beneficiary");
		long end = new Date().getTime();
//		while (res.next()) {
//			System.out.println(res.getString(1) + " ");
//		}
		System.out.println("Query successful in time for MySQL: " + (end-start));

		con.close();
	}
}
