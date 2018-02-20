
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Exp1 {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/input-5k"; 
		String temp1 = "/user/jkuczek/lab4/exp1/temp1";
		String temp2 = "/user/jkuczek/lab4/exp1/temp2";
		String output = "/user/jkuczek/lab4/exp1/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 10; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Graph Program Round One");

		// Attach the job to this Exp1
		job_one.setJarByClass(Exp1.class);

		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(IntWritable.class);

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
		job_two.setJarByClass(Exp1.class);
		//job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(IntWritable.class);
		job_two.setOutputKeyClass(IntWritable.class);
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
		job_three.setMapOutputKeyClass(IntWritable.class);
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
				String v1 = tokens.nextToken();
				String v2 = tokens.nextToken();
				if(v1 != v2) {
					// separate the input so we have two lists
					// each vertex will be mapped to a list of incoming and outgoing edges
					w1.set(v1);
					w2.set(v2);
					context.write(w2, new Text("I" + w1));
					context.write(w1, new Text("O" + w2));
				}// end if the two vertices are equal
			} // End while
		}// end function map 
	}// end class Map_One
	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String incoming = "N";
			String outgoing = "N";
			for (Text val : values) {
				String value = val.toString();
				if(value.charAt(0) == 'O') {
					// this is an outgoing value
					outgoing += value.substring(1) + ",";
				}else {
					// this is an incoming value
					incoming += value.substring(1) + ",";
				}// end if outgoing value
			}// end for loop computing frequency
			
			// remove the trailing commas from both lists
			incoming = removeLast(incoming);
			outgoing = removeLast(outgoing);
			
			// output the vertex and both lists to the next mapper
			context.write(key, new Text(incoming+"-"+outgoing));
		}// end function reduce
		
		private String removeLast(String s) {
			if(s.length() > 1) {
				// remove the last comma in the string
				return s.substring(0, s.length() - 1);
			}else {
				return s;
			}// end if string length > 0
		}// end function removeTrailingComma
	}// end class Reduce_One

	// The second Map Class
    public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted       	
        	String[] data = value.toString().split("\t");
        	        	
        	// further parse the output from first reduce
        	String vertex = data[0];
        	String[] vertices = data[1].split("-");
        
        	// convert comma separated strings into ArrayLists
        	// we now have the incoming and outgoing vertices in two ArrayLists
        	ArrayList<String> incoming = convert(vertices[0]);
        	ArrayList<String> outgoing = convert(vertices[1]);
        	
        	for(String v : outgoing) {
        		// count all the two-hops
        		context.write(new Text(v), new IntWritable(incoming.size()));
        	}// end foreach loop over all outgoing edges
        	
        	// count all the one-hops
        	context.write(new Text(vertex), new IntWritable(incoming.size()));        	
        } // end function map
        
        private ArrayList<String> convert(String s){
        	if(s.length() > 1) {
        		s = s.substring(1, s.length());
        		return new ArrayList<String>(Arrays.asList(s.split(",")));
        	}else {
        		return new ArrayList<String>();
        	}// end if length > 1
        }// end function convert
    }// end class Map_Two

    // The second Reduce class
    public static class Reduce_Two extends Reducer<Text, IntWritable, IntWritable, Text> {
		
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable count : values) {
            	context.write(count, key);
            }// end for loop adding all elements to priority queue
        }// end function reduce
        
    }// end class Reduce_Two
    
    // The third Map Class
    public static class Map_Three extends Mapper<LongWritable, Text, IntWritable, Text> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted
        	String[] data = value.toString().split("\t");
        	IntWritable count = new IntWritable(Integer.parseInt(data[0]));
        	context.write(count, new Text(data[1]));
        } // end function map
        
    }// end class Map_Three
    
    // The third Reduce class
    public static class Reduce_Three extends Reducer<IntWritable, Text, IntWritable, Text> {
    	
    	private Text word = new Text();
    	LinkedHashMap<Integer, String> queue = new LinkedHashMap<Integer, String>()
        {
		   static final long serialVersionUID=42L;
		   
           @Override
           protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest)
           {
              return this.size() > 10;   
           }
        };
		
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	// add elements to the queue, since reduce sorts by key in ascending order,
        	// the last items to be added to the queue will be the top 10 most significant patents
            for (Text val : values) {
            	if(val != null) {
            		queue.put(new Integer(key.get()), val.toString());
            	}// end if value is not null
            }// end for loop adding all elements to priority queue
        }// end function reduce
      
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
      	  // print out the contexts of the queue once the task is complete
      	  String result = "";
      	  int i = 9;
      	  String[] s = new String[10];
      	
          for (Integer count : queue.keySet()){
          	String vertex = queue.get(count).toString();
          	s[i--] = "Patent: " + vertex + " - Significance: " + count + "\r\n";  
          }// end for loop over the LinkedHashMap
          
          for(i = 0; i < 10; i++) {
          	result += i+1 + ": " + s[i];
          }// end for loop printing out results 
          
          word.set(result);
          context.write(null, word);
        }// end cleanup function
        
    }// end class Reduce_Three

}// end class Exp1
