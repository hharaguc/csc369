// Holly Haraguchi
// CSC 369, Winter 2017
// Lab 7, Program 4: Inverting
// Outputs < word , <numUniqueOccur, numDocsOccur, [docsOccur]> >

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename

// Exception handling
import java.io.IOException;

// Hash map
import java.util.HashMap;

public class invertedIndex {

    // Mapper Class
    // Input: (<messageID>, <message text>)
    // Outputs: (<word> , <frequency, messageID>)
    public static class invertedMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            HashMap<String, Integer> numOccurs = new HashMap();
            String[] vals = value.toString().split(",");

            // Valid input
            if (vals.length == 2) {
                String msgId = vals[0];

                // Skip over the leading space in the message
                String text = vals[1].substring(1);

                // Get rid of punctuation and split the message into words
                String[] words = text.replaceAll("[^a-zA-Z ]", "").split(" ");

                // Initialize the hash map
                for (String word : words) {
                    if (numOccurs.containsKey(word)) {
                        numOccurs.put(word, numOccurs.get(word) + 1);
                    }
                    else {
                        numOccurs.put(word, 1);
                    }
                }

                // Emit the hash map as (key, value) pairs
                for (String word : numOccurs.keySet()) {
                    String outVal = numOccurs.get(word) + "," + msgId;
                    context.write(new Text(word), new Text(outVal));
                }
            }        
        }
    }

    // Reducer Class
    // Input: (<word> , <frequency, messageID>)
    // Outputs: (<word> , <numOccurs, numDocs, [doc IDs]>)
    public static class invertedReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalOccurs = 0;
            int numDocs = 0;
            String docIds = "[";

            // Calculate stats from input
            for (Text val : values) {
                String input = val.toString();
                String[] stats = input.split(",");

                totalOccurs += Integer.parseInt(stats[0]);
                docIds += stats[1] + ", ";
                numDocs++;
            }

            // Replace the final ", " with a closing bracket
            docIds = docIds.substring(0, docIds.length() - 2) + "]";

            context.write(key, new Text(String.valueOf(totalOccurs) + ", " + String.valueOf(numDocs) + ", " + docIds));
        }
    }

    // Section 4: MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: Get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: Register the MapReduce class
        job.setJarByClass(invertedIndex.class);  

        // Step 3: Set I/O files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4: Register mapper and reducer
        job.setMapperClass(invertedMapper.class);
        job.setReducerClass(invertedReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Inverted - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}

