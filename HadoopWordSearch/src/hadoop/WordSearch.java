package hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordSearch extends Configured implements Tool {

	static String find;

	public int run(String[] args) throws Exception {
		// creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(getConf(), WordSearch.class);
		conf.setJobName("WordSearch");
		conf.setNumReduceTasks(2);

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(WordSearchMapperWithCombiner.class);
		//conf.setMapperClass(WordSearchMapper.class);

		conf.setReducerClass(WordSearchReducer.class);
		// We wil give 2 arguments at the run time, one in input path and other
		// is output path
		Path inp = new Path(args[0]);
		Path out = new Path(args[1]);
		// the hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.addInputPath(conf, inp);
		FileOutputFormat.setOutputPath(conf, out);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// this main function will call run method defined above.
		StringBuffer fileList = new StringBuffer();
		int count = 0;
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter search string : ");
		find = scan.nextLine();
		long start = new Date().getTime();
		int res = ToolRunner.run(new Configuration(), new WordSearch(), args);
		long end = new Date().getTime();
		System.out.println("Job done");
		System.out.println("Job took " + (end - start) + "milliseconds");
		
		//i is the total number of reduce tasks
		for (int i = 0; i < 2; i++) 
		{
			try (BufferedReader br = new BufferedReader(new FileReader(
					"/usr/local/hadoop/output/part-0000"+Integer.toString(i)))) {
				String currentLine;
				while ((currentLine = br.readLine()) != null) {
					StringTokenizer tokenizer = new StringTokenizer(currentLine);					
					while (tokenizer.hasMoreTokens()) {
						String name = tokenizer.nextToken();
						String value = tokenizer.nextToken();
						if (Integer.parseInt(value) == 0) {
							fileList.append(name + "\n");
						} else {
							count = Integer.parseInt(value);
						}
					}
				}			

			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		System.out.println("Total number of appearance : " + count);
		System.out.println(fileList);
		System.out.println("Job done");
		System.exit(res);
	}
}