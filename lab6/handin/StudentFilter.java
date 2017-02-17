// Holly Haraguchi
// Adapted from Java Hadoop Template provided by Professor Alex Dekhtyar
// CSC 369, Winter 2017 
// Lab 6 - Student Filter MapReduce Driver 


// Section 1: Imports
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized Long wrapper class

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

// Configuration
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

// DRIVER CLASS
public class StudentFilter {

   // Section 2: Mapper Class Template
   public static class MyMapperClass extends Mapper<Text, Text, Text, Text> {
      @Override
      public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

         final String outName = "Holly Haraguchi";
         
         String text[] = value.toString().split(",");

         if (text.length == 2) {
            String name = text[0];
         
            // Analyze the input 
            Text outKey = new Text(name);
            Text outValue = new Text(outName);

            if (name.charAt(0) == outName.charAt(0)) {
               context.write(outKey, outValue);
            }   
         } 
      } 
   } // MyMapperClass


   // Section 3: Reducer Class Template
   public static class MyReducerClass extends Reducer<Text,  Text, Text, Text> {
      @Override
      public void reduce( Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         String name = "";
  
         // Iterate over all values in the provided Iterable
         for (Text val : values) {
            name = val.toString();
         } 

         // Emit final output
         context.write(key, new Text(name));   
      } 
   } 


   // Section 4:  MapReduce Driver
   public static void main(String[] args) throws Exception {
      // Step 0: Config
      Configuration conf = new Configuration();
      conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
  
      // Step 1: get a new MapReduce Job object
      Job job = Job.getInstance(conf);  
     
      // Step 2: register the MapReduce class
      job.setJarByClass(StudentFilter.class);  

      // Step 3:  Set Input and Output files
      FileInputFormat.setInputPaths(job, new Path("/data/", "students.csv")); 
      FileOutputFormat.setOutputPath(job, new Path("./test/", "output")); 
      job.setInputFormatClass(KeyValueTextInputFormat.class); 

      // Step 4:  Register mapper and reducer
      job.setMapperClass(MyMapperClass.class);
      job.setReducerClass(MyReducerClass.class);
  
      // Step 5: Set up output information
      job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
      job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

      // Step 6: Set up other job parameters at will
      job.setJobName("Filter Student List - Holly Haraguchi");

      // Step 7:  ?
   
      // Step 8: profit
      System.exit(job.waitForCompletion(true) ? 0:1);
  } 
} // MyMapReduceDriver

