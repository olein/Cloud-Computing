package hadoop;

import hadoop.WordSearch;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordSearchMapperWithCombiner extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	// hadoop supported data types
	//private final static IntWritable one = new IntWritable(1);
	private final static IntWritable fileDetector = new IntWritable(0);
	private Text word = new Text();
	private Text file = new Text();
	int count = 0;	

	private TreeMap<String, Integer> words = new TreeMap<String, Integer>();

	// map method that performs the tokenizer job and framing the initial key
	// value pairs
	// after all lines are converted into key-value pairs, reducer is called.
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// taking one line at a time from input file and tokenizing the same
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		// combiner part
		// iterating through all the words available in that line and forming
		// the key value pair
		words.clear();
		while (tokenizer.hasMoreTokens()) {
			String match = tokenizer.nextToken();
			if (match.equals(WordSearch.find)) {
				if (words.containsKey(match)) {
					count = (int) words.get(match);
					words.put(match, count + 1);
				} else {
					words.put(match, 1);
					count = 1;
				}

				String fileName = ((FileSplit) reporter.getInputSplit())
						.getPath().getName();
				System.out.println(fileName);

				file.set(fileName);
				output.collect(file, fileDetector);
			}
		}
		for(String keys: words.keySet())
		{
			count = words.get(keys);
			System.out.println("Total :"+count);
			final IntWritable values = new IntWritable(count);
			word.set(keys);
			output.collect(word, values);
		}
	}
}