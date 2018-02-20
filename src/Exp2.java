
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Exp2 {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		//String input = "/user/jkuczek/input"; 
		String input = "/cpre419/patents.txt"; 
		String temp1 = "/user/jkuczek/lab3/exp2/temp1";
		String temp2 = "/user/jkuczek/lab3/exp2/temp2";
		String output = "/user/jkuczek/lab3/exp2/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 1; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Graph Program Round One");

		// Attach the job to this Exp1
		job_one.setJarByClass(Exp2.class);

		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));
		
		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		Job job_two = Job.getInstance(conf, "Graph Program Round Two");
		job_two.setJarByClass(Exp2.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);
				
		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp1));
		FileOutputFormat.setOutputPath(job_two, new Path(temp2));

		// Run the job
		job_two.waitForCompletion(true);

		// Create job for round 3
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		Job job_three = Job.getInstance(conf, "Graph Program Round Three");
		job_three.setJarByClass(Exp1.class);
		job_three.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_three.setMapOutputKeyClass(Text.class);
		job_three.setMapOutputValueClass(Text.class);
		job_three.setOutputKeyClass(Text.class);
		job_three.setOutputValueClass(Text.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_three.setMapperClass(Map_Three.class);
		job_three.setReducerClass(Reduce_Three.class);
				
		job_three.setInputFormatClass(TextInputFormat.class);
		job_three.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(temp2));
		FileOutputFormat.setOutputPath(job_three, new Path(output));

		// Run the job
		job_three.waitForCompletion(true);
	}// end function main

	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text w1 = new Text();
		private Text w2 = new Text();
		
		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
						
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(line);

			while (tokens.hasMoreTokens()) {
				w1.set(tokens.nextToken());
				w2.set(tokens.nextToken());
				
				if(!w1.equals(w2)) {
					context.write(w1, w2);
					context.write(w2, w1);
				}// end if two vertices are not equal
			} // End while
		}// end function map 
	}// end class Map_One
	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
		
		private Text w1 = new Text();
		private Text w2 = new Text();
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			w1.set(key);
			w2.set(toList(values));
			context.write(w1, w2);
		}// end function reduce
		
		public String toList(Iterable<Text> values) {
			String result = "";
			for(Text value : values) {
				result += value + ",";
			}// end foreach loop over values
			return result.substring(0, result.length()-1);
		}// end function toList
		
	}// end class Reduce_One

	// The second Map Class
    public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted
        	String[] data = value.toString().split("\t");
 
        	// pull relevant vertex data from input
        	String vertex = data[0];
        	ArrayList<String> neighbors = toList(data[1]);
        	
        	for(int i = 0; i < neighbors.size(); i++) {
        		// loop through the neighbor list
        		String v = neighbors.get(i);
        		context.write(new Text(v), new Text(vertex+"-"+String.join(",", neighbors)));
        	}// end for loop over all neighbors
        	
        } // end function map
        
        private ArrayList<String> toList(String s) {
        	ArrayList<String> list = new ArrayList<String>(Arrays.asList(s.split(",")));
        	Collections.sort(list);
        	return list;
        }// end function convert
        
    }// end class Map_Two

    // The second Reduce class
    public static class Reduce_Two extends Reducer<Text, Text, Text, Text> {
    	
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String v1 = key.toString();
        	HashSet<String> neighbors = new HashSet<String>();
        	HashMap<String, HashSet<String>> neighborMap = new HashMap<String, HashSet<String>>();
        	
        	for(Text value : values) {
        		// parse the input data from Map_Two
        		String[] data = value.toString().split("-");
        		String v2 = data[0];
        		neighbors = toSet(v1, data[1]);
        		neighborMap.put(v2, neighbors);
        	}// end foreach loop over values
        	
        	int triangles = 0;
        	int triplets = 0;
        	for(String k : neighborMap.keySet()){
        		neighbors = neighborMap.get(k);
        		if(!k.equals(v1)){
            		for(String edgeNode : neighbors){
            			if(neighborMap.containsKey(edgeNode)){
            				triangles++;
            			}// end if we found a triangle
            			triplets++;
            		}// end for loop over all neighbors
        		}// end if unique vertex key
        	}// end for loop over all keys
        	
        	context.write(new Text(v1), new Text(triangles+","+triplets));
        }// end function reduce
        
        private HashSet<String> toSet(String toRemove, String s) {
        	HashSet<String> values = new HashSet<String>();
        	StringTokenizer st = new StringTokenizer(s, ",");
        	
        	while(st.hasMoreTokens()){
        		String token = st.nextToken();
        		if(!token.equals(toRemove)){
        			values.add(token);	
        		}// end if we dont want to add this token	
        	}// end while there are more token
        	return values;
        }// end function convert
        
    }// end class Reduce_Two
    
    // The third Map Class
    public static class Map_Three extends Mapper<LongWritable, Text, Text, Text> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted
        	String[] data = value.toString().split("\t");
        	// pass all counts to reduce
        	context.write(new Text("1"), new Text(data[1]));
        } // end function map
        
    }// end class Map_Three
    
    // The third Reduce class
    public static class Reduce_Three extends Reducer<Text, Text, Text, Text> {
    	
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	// add elements to the queue, since reduce sorts by key in ascending order,
        	// the last items to be added to the queue will be the top 10 bigrams
        	int totalTriangles = 0;
        	int totalTriplets = 0;
            for (Text val : values) {
            	String[] counts = val.toString().split(",");
            	
            	// parse the triplets and triangles
            	String triangles = counts[0];
            	String triplets = counts[1];
            	
            	// sum the total triangles and triplets
            	totalTriangles += Integer.parseInt(triangles);
            	totalTriplets += Integer.parseInt(triplets);
            }// end for loop adding all elements to priority queue
            
            String result = "";
            
            // we count each triangle three times per point so we must divide by 6 
            // to get actual number of triangles
            totalTriangles = totalTriangles / 6;
            totalTriplets = totalTriplets / 2;
            
            // calculate gcc
            // create the result output
            double gcc = (double) (3 * totalTriangles) / totalTriplets;
            result += "GCC: " + gcc + "\r\n";
            result += "Triangles: " + totalTriangles + "\r\n";
            result += "Triplets: " + totalTriplets + "\r\n";
            
            context.write(null, new Text(result));
        }// end function reduce
      
    }// end class Reduce_Three

}// end class Exp2
