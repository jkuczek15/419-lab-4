
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exp1 {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/input-50m"; 
		String partitionOutputPath = "/user/jkuczek/lab4/exp1/partition";
		String output = "/user/jkuczek/lab4/exp1/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 10;
		
		// The maximum number of samples for our random input sampler
		int samples = 10000000;
		
		// job configuration
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Sort Exp1");

		// Attach the job to this Exp1
		job_one.setJarByClass(Exp1.class);

		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		
		// The following instructions should be executed before writing the partition file
        job_one.setNumReduceTasks(reduce_tasks);
        TotalOrderPartitioner.setPartitionFile(job_one.getConfiguration(), new Path(partitionOutputPath));
        
        // Write partition file with random sampler
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, samples, 100);
        InputSampler.writePartitionFile(job_one, sampler);
 
        // Use TotalOrderPartitioner and default identity mapper and reducer 
        job_one.setPartitionerClass(TotalOrderPartitioner.class);
        job_one.setMapperClass(Mapper.class);
        job_one.setReducerClass(Reducer.class);
 		
		// Run the job
		job_one.waitForCompletion(true);
	}// end function main

}// end class Exp1
