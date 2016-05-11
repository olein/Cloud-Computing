package hadoop;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class HtmlGenerator {

	public static void main(String args[]) throws IOException {

		
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Number OF file To gen :");
		int numberOfFilesToGen = scan.nextInt();
		
		System.out.println("Enter File Size (in kb):");
		long filesSizeToGen = scan.nextLong();
		
		filesSizeToGen *= 1024;	
		
		int upperRange = 1000000;
		int lowerRange = 0;	
		
		Random rand = new Random();

		for (int i = 0; i < numberOfFilesToGen; i++) {

			String htmlFileName = "/usr/local/hadoop/input/"+"generatedHtml" + i + ".html";

			File file = new File(htmlFileName);
			file.createNewFile();

			FileWriter writer = new FileWriter(file);

			float fileSize = file.length();

			int j = 0;
			while (fileSize < filesSizeToGen) {

				String aa = "";

				if (++j < 10) {
					aa = (rand.nextInt(upperRange)-lowerRange) + " ";					
				} else {
					aa = (rand.nextInt(upperRange)-lowerRange) + "\n";
					j = 0;
				}

				writer.write(aa);

				fileSize = file.length();

			}

			writer.flush();
			writer.close();		

		}
		System.out.println("File generation completed");

	}
}