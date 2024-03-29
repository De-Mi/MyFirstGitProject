import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 



public class WordCount2 {
	

	 public static class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		 System.out.println(value.toString());System.out.println(value.toString().replace(",", ""));
		 StringTokenizer itr = new StringTokenizer(value.toString().replace(",", ""));
		 
		 while (itr.hasMoreTokens()) {
			 word.set(itr.nextToken());
			 context.write(word, one);
		 }
		}
	 }
	 
	 public static class WCPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			return 0;
		}
		 
	 }
	 public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
			 }
			 result.set(sum);
			 context.write(key, result);
		 }
	 } 
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "word count");
		 
		 job.setJarByClass(WordCount2.class);
		 
		 job.setMapperClass(WCMapper.class);
		 job.setPartitionerClass(WCPartitioner.class);
		 job.setCombinerClass(WCReducer.class);
		 job.setReducerClass(WCReducer.class);
		 
		 job.setOutputKeyClass(Text.class);
		 
		 job.setOutputValueClass(IntWritable.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
} 
