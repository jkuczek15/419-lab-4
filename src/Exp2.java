
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 4 *********************
  *****************************************
  *****************************************
  */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exp2 {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		Path input = new Path("/cpre419/input-5m");
		Path output = new Path("/user/jkuczek/lab4/exp2/output/"); 

		// The number of reduce tasks 
		int reduce_tasks = 10;
		
		// job configuration
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Sort Exp2");

		// Attach the job to this Exp1
		job_one.setJarByClass(Exp2.class);

		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, input);
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, output);
		
		// The following instructions should be executed before writing the partition file
        job_one.setNumReduceTasks(reduce_tasks);
 
        // Use TotalOrderPartitioner and default identity mapper and reducer 
        job_one.setPartitionerClass(CustomPartitioner.class);
        job_one.setMapperClass(Mapper.class);
        job_one.setReducerClass(Reducer.class);
 		
		// Run the job
		job_one.waitForCompletion(true);
	}// end function main
	
	public static class CustomPartitioner extends Partitioner<Text, Text> {
		
	    @Override
	    public int getPartition(Text k, Text value, int numPartitions) {
	    	// Alphabet is hard-coded, we change the alphabet when we want to sort more types of strings
	    	// An alternative way of doing this would be scanning once through the file using a MapReduce job
	    	// to collect all the possible characters for a key
	    	String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	    	String key = k.toString();
	    	
	    	// determine the length of each split
	    	int splitLength = alphabet.length() / numPartitions;
	    	
	    	// Initialize variables before we begin the loop to determine partition
	    	int i = 0;
	    	int j = splitLength;
	    	int partition = 0;
	    	
	    	// Loop to determine the correct partition, the main idea here
	    	// is to check different String intervals
	    	// If the key falls within a certain interval, we return the interval's partition
	    	// this can lead to bad performance if our input is very similar
	    	while(partition < numPartitions-1) {
	    		// Build filled strings from the character indexes 
	    		// to make comparisons
	    		String greater = fill(key.charAt(i), key.length());
	    		String less = fill(key.charAt(j), key.length());
	    		
	    		// Compare the key to the two filled strings
	    		if(key.compareTo(greater) >= 0 && key.compareTo(less) < 0) {
	    			return partition;
	    		}// end if we found our partition
	    		
	    		// Increment loop counters
	    		i = j;
	    		j += splitLength;
	    		partition++;
	    	}// end while loop returning partitions
	    	
	    	return partition;
	    }// end function getPartition
	    
	    /*
	     * Returns a string completely filled with character 'c' of length 'l'
	     * Ex: fill('0', '5') => '00000'
	     */
	    public String fill(char c, int l) {
	    	String s = "";
	    	for(int i = 0; i < l; i++) {
	    		s += c;
	    	}// end for loop creating string
	    	return s;
	    }// end function fill
	    
	}// end class CustomPartitioner

}// end class Exp2
