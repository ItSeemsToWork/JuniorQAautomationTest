import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sun.rowset.CachedRowSetImpl;


public class Main {
	
	private static Connection сonnection;
	
	private static String dburl = "jdbc:postgresql://localhost:5432/TestDB";
	private static String dbuser = "postgres";
	private static String dbpass = "test";
	
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
				
		final String sql = "SELECT department, SUM(salary)AS salarysum "+
		                   "FROM employees "+
		                   "GROUP BY department";
		
		try {
			ResultSet resultSet = dbExecute(sql);
			while (resultSet.next()) {
				System.out.println(resultSet.getString("department") + ": " + resultSet.getString("salarysum"));
			}
		} catch (SQLException e) {
			System.err.println("ERROR! НЕ МОЖЕТ ПОЛУЧИТЬ ДАННЫЕ ПО ТАБЛИЦЕ" + e);
			throw e;
		}
	}
	
	public static Connection getDataBaseConnection() throws ClassNotFoundException, SQLException {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("НЕ ВИДИТ ДРОВА");
			e.printStackTrace();
			throw e;
		}

		try {
			сonnection = DriverManager.getConnection(dburl, dbuser, dbpass);
		} catch (SQLException e) {
			System.err.println("НЕТ КОННЕКТА: \n+" + e);
		}
		return сonnection;
	}
	
	public static void closeDataBaseConnection() throws SQLException {
		try {
			if (сonnection != null && !сonnection.isClosed())
				сonnection.close();
		} catch (SQLException e) {
			throw e;
		}
	}
	
	public static ResultSet dbExecute(final String sqlQuery) throws ClassNotFoundException, SQLException {
		Statement statement = null;
		ResultSet resultSet = null;
		CachedRowSetImpl crsi = null;
		try {
			getDataBaseConnection();
			statement = сonnection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);
			crsi = new CachedRowSetImpl();
			crsi.populate(resultSet);
		} catch (SQLException e) {
			System.err.println("ERROR in dbExecute method " + e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			closeDataBaseConnection();
		}
		return crsi;
	}
}


