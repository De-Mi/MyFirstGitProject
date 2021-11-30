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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;


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



public class n_Gram {
	
	//FIVEGRAM
	//Inner class Fivegram created
	private static class Fivegram implements WritableComparable<Fivegram> {
		 Text first;
	     Text second;
	     Text third;
	     Text fourth;
	     Text fifth;
	     
	     private Fivegram() {
	         /* 
	        	
	         */        
	         setFivegram(new Text(), new Text(), new Text(), new Text(), new Text());
	     }
	     
		
	     //Constructor
	     private Fivegram(Text first, Text second, Text third, Text fourth, Text fifth) {
			//initialising 5 fivegram objects
			this.first = new Text();
			this.second = new Text();
			this.third = new Text();
			this.fourth = new Text();
			this.fifth = new Text();
		}
	     
	    //Setter
	     private void setFivegram(Text first, Text second, Text third, Text fourth, Text fifth) {
	         
	         //Setting fivegram attributes
	        
	         this.first = first;
	         this.second = second;
	         this.third = third;
	         this.fourth = fourth;
	         this.fifth = fifth;
	     }
	     
	     @Override
	     public void write(DataOutput out) throws IOException {
	         //To write fivegram objects
	         
	         first.write(out);
	         second.write(out);
	         third.write(out);
	         fourth.write(out);
	         fifth.write(out);
	     }
	     
	     @Override
	     public String toString() {
	         //Fivegram format should be: "one two three four five"
	         return first.toString() + " " +
	                 second.toString() + " " +
	                 third.toString() +" "+ fourth.toString() +" " +fifth.toString();
	     }

		@Override
		public void readFields(DataInput in) throws IOException {
			
			//To read in values.     
	        first.readFields(in);
	        second.readFields(in);
	        third.readFields(in);
	        fourth.readFields(in);
	        fifth.readFields(in);
		}

		@Override
		public int compareTo(Fivegram o) {
			
			 
	       //Perform a bytewise comparison of two objects 
	        
	        
	        int comp = first.compareTo(o.first);
	        
	        if (comp != 0) {
	            return comp;
	        }
	        
	        comp = second.compareTo(o.second);
	        
	        if (comp != 0) {
	            return comp;
	        }
	        
	        comp = third.compareTo(o.third);
	        
	        if (comp != 0) {
	            return comp;
	        }
	        
	        comp = fourth.compareTo(o.fourth);
	        if (comp != 0) {
	            return comp;
	        }
	        
	        return fifth.compareTo(o.fifth);
		}
		
		@Override
	    public boolean equals(Object o) {
	       
	       //Checks if two instance of Fivegram are the same
	        
	        
	        if (o instanceof Fivegram) { 
	        	Fivegram fivegram = (Fivegram) o;

	            return first.equals(fivegram.first) && second.equals(fivegram.second)
	                && third.equals(fivegram.third) && fourth.equals(fivegram.fifth) && fifth.equals(fivegram.fifth);
	        }
	        return false;
	        
	        
	        
	    }
	    /*
	    @Override
	    public int hashCode() {
	        
	        //To ensure the value is unique for a Trigram with a particular set of attributes.
	        
	       
	        
	        return first.hashCode()*163 + second.hashCode() + third.hashCode() + fourth.hashCode() + fifth.hashCode();
	    }
	    */
		
	     
	}
	
	
	//MAPREDUCE
	public static class N_Gram_Mapper extends Mapper<Object, Text, Fivegram, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static Fivegram fivegram = new Fivegram();
    private Text first = new Text();
    private Text second = new Text();
    private Text third = new Text();
    private Text fourth = new Text();
    private Text fifth = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
       
        
        String line = value.toString().toLowerCase(); //each line converted to lowercase
        
        line = line.replace("\\s+","");         // remove whitespace
              
       
        String[] sentence = line.split("\\s+");             // split line into array by space
        
        //removes whitespace at the beginning of each line
        LinkedList<String> word= new LinkedList<String>(Arrays.asList(sentence)); 
        word.removeFirst();
        sentence = word.toArray(new String[word.size()]);
        
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
            fivegram.setFivegram(first, second, third, fourth, fifth); //fivegram created
          
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
	        
	        
	        job.setJarByClass(n_Gram.class);
	        
	        job.setMapperClass( N_Gram_Mapper.class);
	        job.setMapOutputKeyClass(Fivegram.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        
	        job.setReducerClass(N_Gram_Reducer.class);
	        job.setCombinerClass(N_Gram_Reducer.class);      //combiner / semi-reducer  
	        job.setOutputKeyClass(Fivegram.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	        
	    }

}
