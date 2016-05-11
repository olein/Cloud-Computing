package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class WordCount extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
            //creating a JobConf object and assigning a job name for identification purposes
            JobConf conf = new JobConf(getConf(), WordCount.class);
            conf.setJobName("WordCount");

            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            //Providing the mapper and reducer class names
            conf.setMapperClass(WordCountMapper.class);
            conf.setReducerClass(WordCountReducer.class);
            //We wil give 2 arguments at the run time, one in input path and other is output path
            Path inp = new Path(args[0]);
            Path out = new Path(args[1]);
            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(conf, inp);
            FileOutputFormat.setOutputPath(conf, out);

            JobClient.runJob(conf);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
            // this main function will call run method defined above.
    	  	int res = ToolRunner.run(new Configuration(), new WordCount(),args);
            System.exit(res);           
      }
}