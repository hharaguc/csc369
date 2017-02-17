// CSC 369: Distributed Computing
// Alex Dekhtyar

// Java Hadoop Template


// Section 1: Imports


// Data containers for Map() and Reduce() functions

// You would import the data types needed for your keys and values
import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class


// For Map and Reduce jobs

import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function

// To start the MapReduce process

import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver

// For File "I/O"

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename


// Exception handling

import java.io.IOException;

// The DRIVER CLASS

public class MyMapReduceDriver {


// Section 2. Mapper  Class Template


public static class MyMapperClass     // Need to replace the four type labels there with actual Java class names
     extends Mapper< LongWritable, Text, LongWritable, Text> {

 @Override   // we are overriding Mapper's map() method

// map methods takes three input parameters
// first parameter: input key 
// second parameter: input value
// third parameter: container for emitting output key-value pairs

public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

   // put your map() code in here
   String name = "";
   String text[] = value.toString().split(",");
   
   // analyze input 
   if (text.length == 3) {
      name = text[1];
      String newKey = text[0];
      int myKey = Long.parseLong(newKey);
      
      if (name.charAt(0) == 'H') {
         name = "Holly Haraguchi";
      }
      Text out = new Text(name);
      LongWritable outKey = new LongWritable(myKey);
      context.write(outKey, out);
   }
   // emit output. Use the following for emitting output:



 } // map


} // MyMapperClass


// Section 3: Reducer Class Template

public static class MyReducerClass   // needs to replace the four type labels with actual Java class names
      extends  Reducer< LongWritable,  Text, LongWritable, Text> {

 // note: InValueType is a type of a single value Reducer will work with
 //       the parameter to reduce() method will be Iterable<InValueType> - i.e. a list of these values

@Override  // we are overriding the Reducer's reduce() method

// reduce takes three input parameters
// first parameter: input key
// second parameter: a list of values associated with the key
// third parameter: container  for emitting output key-value pairs

public void reduce( LongWritable key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

  // Your code goes here 

  String name = "";
  // roll through all values in the provided Iterable
  for (Text val : values) {
  
     // do the work
     name = val.toString();
  } // for

  // emit final output
  context.write(key, new Text(name));   


 } // reduce


} // reducer




//Section 4:  MapReduce Driver

// we do everything here in main()
  public static void main(String[] args) throws Exception {

     // step 1: get a new MapReduce Job object
     Job  job = Job.getInstance();  //  job = new Job() is now deprecated
     
    // step 2: register the MapReduce class
      job.setJarByClass(MyMapReduceDriver.class);  

   //  step 3:  Set Input and Output files
       FileInputFormat.setInputPaths(job, new Path(args[0])); // put what you need as input file
       FileOutputFormat.setOutputPath(job, new Path(args[1])); // put what you need as output file

   // step 4:  Register mapper and reducer
      job.setMapperClass(MyMapperClass.class);
      job.setReducerClass(MyReducerClass.class);
  
   //  step 5: Set up output information
       job.setOutputKeyClass(LongWritable.class); // specify the output class (what reduce() emits) for key
       job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

   // step 6: Set up other job parameters at will
      job.setJobName("My Test Job");

   // step 7:  ?
   
   // step 8: profit
      System.exit(job.waitForCompletion(true) ? 0:1);


  } // main()


} // MyMapReduceDriver

