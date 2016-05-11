package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordSearchMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	// hadoop supported data types
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	private Text word = new Text();
	private Text file = new Text();

	// map method that performs the tokenizer job and framing the initial key
	// value pairs
	// after all lines are converted into key-value pairs, reducer is called.
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// taking one line at a time from input file and tokenizing the same
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		// iterating through all the words available in that line and forming
		// the key value pair		

		while (tokenizer.hasMoreTokens()) {
			String match = tokenizer.nextToken();
			// word.set(tokenizer.nextToken());
			if (match.equals((WordSearch.find))) {
				word.set(match);
				// sending to output collector which inturn passes the same to
				// reducer
				output.collect(word, one);
				
				String fileName = ((FileSplit) reporter.getInputSplit()).getPath().getName();
				System.out.println(fileName);
				
				file.set(fileName);
				output.collect(file, zero);
			}
		}
	}
}