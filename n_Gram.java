/**
 * 21COC105 CLOUD COMPTUING COURSEWORK
 * Module Leader: Dr Posco Tso
 * 
 * Written By: B817199 
 *
 * 
 * Input Directory: gs://coc123cw/coc105-gutenburg-10000books/ 
 * Output Directory: gs://coc123cw/output1/ 

**/



import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.idryman.combinefiles.CFRecordReader;
//import org.idryman.combinefiles.FileLineWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class n_Gram extends Configured implements Tool{

	public class Converger extends CombineTextInputFormat{
		public Converger(){
			// setting block size to 128mb which is the Cloudera default HDFS size
			this.setMaxSplitSize(134217728L);
		}
	}
	//MAPREDUCE
    public static class N_Gram_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text first = new Text();
    private Text second = new Text();
    private Text third = new Text();
    private Text fourth = new Text();
    private Text fifth = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
       
        
        String[] sentence = value.toString().toLowerCase().replace("[^a-z\\s", "").trim().split("\\s+");//; //each line converted to lowercase
        
        
        int len = sentence.length;                       
        
        //converting line into fivegram
        for(int i = 0; i+4 < len; i++) {
            
       
            if(len <= 1) {
                continue;
            }
            
            first.set(sentence[i]);
            second.set(sentence[i+1]);
            third.set(sentence[i+2]);
            fourth.set(sentence[i+3]);
            fifth.set(sentence[i+4]);
            Text five_gram = new Text();
            five_gram.set( "{"+first+ " " + second + " " +third + " " +fourth + " " +fifth+",");;

          System.out.println("Map Done");
            context.write(five_gram, one);                
        }
    }
}
	 public static class N_Gram_Reducer
     extends Reducer<Text, IntWritable, Text, IntWritable> {
     
     private IntWritable result = new IntWritable();
     
     @Override
     public void reduce(Text key, Iterable<IntWritable> values, Context context
         ) throws IOException, InterruptedException {
                     
         int sum = 0;
         
         for(IntWritable value : values) {
             sum += value.get();
         }
         result.set(sum);                
         System.out.println("Reduce Done");
         context.write(key, result);
         
     }
 }
	//public class CombinedInputFormat extends CombineTextInputFormat{
	//67108864
 
	
	
	/*public class CombinedInputFormat extends CombineFileInputFormat{
		public CombinedInputFormat(){
		// setting block size to 128mb which is the Cloudera default HDFS size
			this.setMaxSplitSize(134217728L);
		}
	}*/
	
	
	 public static void main(String[] args) throws Exception {
	        ToolRunner.run(new FileMerge(), args);
	 }
	@Override
	public int run(String[] args) throws Exception{
	        Configuration conf = new Configuration();
	       conf.setBoolean("mapreduce.map.output.compress", true);
	        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
	       conf.set("mapred.output.compress", "true");
	        conf.set("mapred.output.compression.type", "BLOCK");
	        conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
	        Job job = Job.getInstance(conf, "Fivegram");
	        
		conf.set("mapreduce.input.fileinputformat.split.maxsize","134217728L");
	conf.setInt(“mapreduce.job.jvm.numtasks”, -1);
	job.setInputFormatClass(CombineTextInputFormat.class);
			//Job job = Job.getInstance();
			//job.setJobName("n_Gram");

			//job.getConfiguration().set("mapreduce.app-submission.cross-platform", "true");
	        
	        job.setJarByClass(n_Gram.class);
	        
	        job.setMapperClass( N_Gram_Mapper.class);
	        //job.setMapOutputKeyClass(Fivegram.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        
	        job.setReducerClass(N_Gram_Reducer.class);
	        job.setCombinerClass(N_Gram_Reducer.class);      //combiner / semi-reducer  
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	       // job.setInputFormatClass(Converger.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
	        return job.waitForCompletion(true) ? 0 : 1;
	        
	    

}
	}
