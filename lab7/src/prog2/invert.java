// Holly Haraguchi
// CSC 369, Winter 2017
// Lab 7, Program 2: Transformation
// Outputs < orig. word , orig. word reversed >

// Section 1: Imports
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object

// Exception handling
import java.io.IOException;

public class invert {

    // Mapper Class
    public static class invertMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {   
        	String word = value.toString();
        	String rev = new StringBuilder(word).reverse().toString();

        	context.write(value, new Text(rev));
        }
    }

    // Reducer Class
    public static class invertReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(invert.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(invertMapper.class);
        job.setReducerClass(invertReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Invert Letters - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
 
