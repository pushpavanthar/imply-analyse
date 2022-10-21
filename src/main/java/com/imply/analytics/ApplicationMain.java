package com.imply.analytics;

import java.io.FileNotFoundException;
import java.util.Scanner;

public class ApplicationMain {

	public static void main(String[] args) throws InterruptedException {

		UserSessionController controller = new UserSessionController(args[0]);
		controller.initialize();

		while(true){
			try {
				System.out.println("Enter the number of distinct paths you want to filter for : ");
				Scanner scanner = new Scanner(System.in);
				String line = scanner.nextLine();
				if(line.chars().allMatch(Character::isDigit)) {
					controller.generateFilteredUsers(Integer.valueOf(line));
				} else {
					break;
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

	}
	
}
