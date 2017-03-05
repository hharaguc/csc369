// CSC 369: Distributed Computing
// Alex Dekhtyar
// Multiple input files

// Section 1: Imports
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // key-value input files
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

public class multiInMR extends Configured implements Tool {

// We have TWO mapper classes, one per input file.

// Mapper for movies.csv file
public static class UserMapper     
     extends Mapper< LongWritable, Text, Text, Text > {

public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        if (splits.length == 2) {
           context.write(new Text(splits[0]), new Text(splits[1]));
        }

 } // map

}  // mapper class

// Mapper for JSON file 
public static class MessageMapper     
     extends Mapper< LongWritable, Text, Text, Text > {


public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
try {
                ArrayList<Double> ratings = new ArrayList<Double>();

                // Create a parser
                JSONParser parser = new JSONParser();
                // Get the JSON string from input and  create a json object
                Object obj = parser.parse(value.toString());
                // Cast to a JSONObject
                JSONObject jsonObj = (JSONObject) obj;
                String rid = (String) jsonObj.get("RID");
                // Get the value of the user's state and their list of ratings
//                String state = "CA";//(String) jsonObj.get("respondent.state");
                JSONArray ratingsArr = (JSONArray) jsonObj.get("ratings");
                Iterator<Double> iter = ratingsArr.iterator();
                while (iter.hasNext()) {
                   ratings.add(iter.next());
                }
                
                for (int i = 0; i < ratings.size(); i++) {
                    context.write(new Text(Integer.toString(i+1)), new Text("rating" + "," + ratings.get(i)));
                }    
                // Output each rating
/*
                for (int i = 0; i < ratings.length; i++) {
                    context.write(new Text(Integer.toString(i+1)), new Text(state + "," + Double.toString(ratings[i])));
                }    
*/}
catch (Exception e) {
   System.out.println("ERROR: " + e.toString());
}

 } // map

} // MyMapperClass


//  Reducer: we only need one reducer class

public static class JoinReducer 
      extends  Reducer< Text, Text, Text, Text> {


public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {
  ArrayList<String> ratings = new ArrayList<String>();
  String movieTitle = "";

  for (Text val : values) {
    String[] splits = val.toString().split(",");

    // Dealing with a rating
    if (splits.length == 2) {
       ratings.add(splits[1]);       
    }
    else {
       movieTitle = splits[0];
    }
  }
    for (String r : ratings) {
    context.write(new Text(movieTitle), new Text(r));
    } 

 } // reduce

} // reducer


//  MapReduce Driver


  public int run(String[] args) throws Exception {

      Configuration conf = super.getConf(); 

     Job  job = Job.getInstance(conf);  
     job.setJarByClass(multiInMR.class);  

   //  Get Multiple Inputs set up.
       MultipleInputs.addInputPath(job, new Path("movies.csv"),
                          TextInputFormat.class, UserMapper.class );
       MultipleInputs.addInputPath(job, new Path("favoriteMovie-input.json"),
                          TextInputFormat.class, MessageMapper.class ); 
 
      FileOutputFormat.setOutputPath(job, new Path("./test/","movieTest-out")); // put what you need as output file

      job.setReducerClass(JoinReducer.class);
      job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
      job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

   // step 6: Set up other job parameters at will
      job.setJobName("Reduce Side Join");

   // step 7:  ?

   // step 8: profit
      return job.waitForCompletion(true) ? 0:1;


  } // main()

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();

      int res = ToolRunner.run(conf, new multiInMR(), args);
      System.exit(res);
   }

} // MyMapReduceDriver




