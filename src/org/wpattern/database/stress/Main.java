package org.wpattern.database.stress;

import java.util.Scanner;

import org.apache.log4j.Logger;

public class Main {

	private static final Logger LOGGER = Logger.getLogger(Main.class);

	public static void main(String[] args) {
		Db db = new Db();
		db.start();
		boolean stop = false;

		Scanner scanner = new Scanner(System.in);

		do {
			try {
				printOptions();

				String option = scanner.nextLine().toLowerCase();

				switch (option) {
				case "c":
					stop = true;
					break;

				case "i":
					System.out.println("Reading the interval (integer) in milleseconds.");

					db.setIntervalMs(Integer.parseInt(scanner.nextLine()));
					break;
				default:
					System.out.println("Option not founded.");
					break;
				}
			} catch(Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		} while (!stop);

		db.stop();
		scanner.close();
	}

	private static void printOptions() {
		System.out.println("C = Close.");
		System.out.println("I = Interval.");
	}

}
