// Section 1: Imports
import java.util.ArrayList;
import java.util.ListIterator;

import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class

import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function

import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input files
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input files

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // key-value input files

import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path; // Hadoop's implementation of directory path/filename

// Exception handling
import java.io.IOException;

// JSON Simple
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

public class favoriteMovie extends Configured implements Tools {
    // Mapper for movies.csv file
    // Input: (<movieId> , <movieTitle>)
    // Output: (<movieId>, <movieTitle>)
    public static class MovieMapper extends Mapper< Text, Text, Text, Text > {
        @Override
        public void map(Text key, Text value, Context context)throws IOException, InterruptedException {
            context.write(key, value);
        } 
    } 

    // Mapper for favoriteMovies.json file
    // Input: (<filePosn> , <movieRating JSON object>)
    // Output: (<movieId>, <respondent info>)
    public static class JsonMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
            try {    
                // Create a parser
                JSONParser parser = new JSONParser();
                // Get the JSON string from input and  create a json object
                Object obj = parser.parse(value.toString());
                // Cast to a JSONObject
                JSONObject jsonObj = (JSONObject) obj;
                // Get the value of the user's state and their list of ratings
                String state = "CA";//(String) jsonObj.get("respondent.state");
                // Double[] ratings = (Double[]) jsonObj.get("ratings");
                String rid = Integer.toString((int)jsonObj.get("RID"));
                // Output each rating
                for (int i = 0; i < 11; i++) {
                    context.write(new Text(Integer.toString(i+1)), new Text(state + "," + rid));
                }    
            } 
            catch (Exception e) {
                // Handle error here
                System.out.println("ERROR:" + e.toString());
                System.exit(1);
            }           
        } 
    } 

    public static class Reducer1 extends  Reducer< Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public int run(String[] args) throws Exception {
        // Step 0: Configuration
        Configuration conf = super.getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

        // Step 1: Get a new MapReduce Job object
        Job job = Job.getInstance(conf, "favoriteMovie");  

        // Step 2: Register the MapReduce class
        job.setJarByClass(favoriteMovie.class);  

        // Step 3: Set up multiple inputs and single output
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class, JsonMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Step 4: Register reducer
        job.setReducerClass(Reducer1.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Prog 3, Step 1 - Holly Haraguchi + Kevin Costello");
        
        // Step 7
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new favoriteMovie(), args);
        System.exit(res);
    }
}
