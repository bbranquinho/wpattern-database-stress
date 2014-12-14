package org.wpattern.database.stress;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class Db {

	private static final Logger LOGGER = Logger.getLogger(Db.class);

	private static final int NUM_THREADS = 8;

	// Define when each thread must stop.
	private volatile static boolean stopThreads = true;

	// Interval in milliseconds between each query execution.
	private volatile static int intervalMs = 100;

	private Thread[] pool;

	public void start() {
		stopThreads = false;

		this.pool = new Thread[NUM_THREADS];

		for (int i = 0; i < this.pool.length; i++) {
			this.pool[i] = new Thread(new DbRunnable(), "stress-db-" + i);
		}

		for (int i = 0; i < this.pool.length; i++) {
			this.pool[i].start();
		}
	}

	public void stop() {
		System.out.println("Stop all threads.");

		stopThreads = true;

		for (Thread thread : this.pool) {
			thread.interrupt();
		}
	}

	public void setIntervalMs(int intervalMs) {
		Db.intervalMs = intervalMs;
		System.out.println(String.format("Changed the interval to [%s].", intervalMs));
	}

	// Database query.
	private static final String QUERY = "SELECT * FROM categories";

	// Driver used to connect with the database.
	private static final String DRIVER = "com.mysql.jdbc.Driver";

	// Database URL.
	private static final String URL = "jdbc:mysql://localhost:3306/wproject";

	// Database username.
	private static final String USERNAME = "wpattern";

	// Database password.
	private static final String PASSWORD = "123456";

	public Statement connectToMySQL() {

		try {
			Class.forName(DRIVER);
			java.sql.Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			return con.createStatement();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

	private class DbRunnable implements Runnable {

		@Override
		public void run() {
			System.out.println(String.format("Starting the thread [%s].", Thread.currentThread().getName()));
			Statement connection = Db.this.connectToMySQL();

			while (!stopThreads) {
				try {
					ResultSet result = connection.executeQuery(QUERY);

					while (result.next()) {
						if (LOGGER.isInfoEnabled()) {
							Long categoryId = result.getLong("CategoryID");
							String categoryName = result.getString("CategoryName");
							String description = result.getString("Description");

							LOGGER.info(String.format("Category [ID = %s, Name = %s, Description = %s]",
									categoryId, categoryName, description));
						}
					}
				} catch (SQLException e) {
					LOGGER.error(e.getMessage(), e);
				}

				try {
					if (intervalMs > 0) {
						Thread.sleep(intervalMs);
					}
				} catch (InterruptedException e) {
					LOGGER.info(e.getMessage());
				}
			}

			System.out.println(String.format("Ending the thread [%s].", Thread.currentThread().getName()));
		}

	}

}
