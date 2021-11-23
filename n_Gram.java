import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class n_Gram {
	
	public static class Fivegram implements WritableComparable<Fivegram> {
		 Text first;
	     Text second;
	     Text third;
	     Text fourth;
	     Text fifth;
	     
	     public Fivegram() {
	         /* 
	         Instantiation without params creates a trigram with
	         three empty Text objects 
	         */        
	         set(new Text(), new Text(), new Text(), new Text(), new Text());
	     }
	     
		
	     //Constructor
	     public Fivegram(Text first, Text second, Text third, Text fourth, Text fifth) {
			//super();
			this.first = new Text();
			this.second = new Text();
			this.third = new Text();
			this.fourth = new Text();
			this.fifth = new Text();
		}
	     
	    //Setter
	     public void set(Text first, Text second, Text third, Text fourth, Text fifth) {
	         /* 
	         Sets the three fivegram attributes
	         */
	         this.first = first;
	         this.second = second;
	         this.third = third;
	         this.fourth = fourth;
	         this.fifth = fifth;
	     }
	     
	     @Override
	     public void write(DataOutput out) throws IOException {
	         /*
	         Method to write out Trigram objects. 
	         Notice that hadoop's Text class makes it easy via it's 
	         write() method!
	         */
	         first.write(out);
	         second.write(out);
	         third.write(out);
	         fourth.write(out);
	         fifth.write(out);
	     }
	     
	     @Override
	     public String toString() {
	         /* 
	         Convert a Trigram instance to a string of comma 
	         separated values: "first, second, third"
	         */
	         return first.toString() + " " +
	                 second.toString() + " " +
	                 third.toString() +" "+ fourth.toString() +" " +fifth.toString() + "; Number of Occurrence: ";
	     }

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			/*
	        Method to read in values.
	        
	        Once again, Hadoop makes it easy to read in values via the 
	        Text class's readFields() method. 
	        */
	        first.readFields(in);
	        second.readFields(in);
	        third.readFields(in);
	        fourth.readFields(in);
	        fifth.readFields(in);
		}

		@Override
		public int compareTo(Fivegram o) {
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			 /*
	        Method to perform a bytewise comparison of
	        two objects. Method returns a non-zero
	        integer value when two objects are not equal. 
	        */
	        
	        int compared = first.compareTo(o.first);
	        
	        if (compared != 0) {
	            return compared;
	        }
	        
	        compared = second.compareTo(o.second);
	        
	        if (compared != 0) {
	            return compared;
	        }
	        
	        compared = third.compareTo(o.third);
	        
	        if (compared != 0) {
	            return compared;
	        }
	        
	        compared = fourth.compareTo(o.fourth);
	        if (compared != 0) {
	            return compared;
	        }
	        
	        return fifth.compareTo(o.fifth);
		}
		
		
	    
		
	     
	}
	
	public static class N_Gram_Mapper extends Mapper<Object, Text, Fivegram, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static Fivegram fivegram = new Fivegram();
    private Text first = new Text();
    private Text second = new Text();
    private Text third = new Text();
    private Text fourth = new Text();
    private Text fifth = new Text();
    
    @Override
    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        
        // Create our string and list of words
        
        String line = value.toString(); //.toLowerCase();   // create string and lower case
        
        line = line.replaceAll("[^a-z\\s]","");         // remove bad non-word chars
                
        String[] words = line.split("\\s");             // split line into list of words
        
        int len = words.length;                         // need the length for our loop condition
        
        for(int i = 0; i+4 < len; i++) {
            
            /*
            short lines lines to produce trigrams of # # #
            such as when we have length of 5 (# # the # #).                
            */
            if(len <= 1) {
                continue;
            }
            
            first.set(words[i]);
            second.set(words[i+1]);
            third.set(words[i+2]);
            fourth.set(words[i+3]);
            fifth.set(words[i+4]);
            fivegram.set(first, second, third, fourth, fifth);
            
            context.write(fivegram, one);                
        }
    }
}
	 public static class N_Gram_Reducer
     extends Reducer<Fivegram, IntWritable, Fivegram, IntWritable> {
     
     private IntWritable result = new IntWritable();
     
     @Override
     public void reduce(Fivegram key, Iterable<IntWritable> values, Context context
         ) throws IOException, InterruptedException {
                     
         int sum = 0;
         
         for(IntWritable value : values) {
             sum += value.get();
         }
         result.set(sum);                
         
         context.write(key, result);
                     
     }
 }
 
	
	
	
	
	
	 public static void main(String[] args) throws Exception {
	        
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Fivegram");
	        
	        job.setJarByClass(WordCount.class);
	        
	        job.setMapperClass( N_Gram_Mapper.class);
	        job.setMapOutputKeyClass(Fivegram.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        
	        job.setReducerClass(N_Gram_Reducer.class);
	        job.setCombinerClass(N_Gram_Reducer.class);        
	        job.setOutputKeyClass(Fivegram.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	        
	    }

}
