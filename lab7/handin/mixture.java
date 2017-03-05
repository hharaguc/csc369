// Holly Haraguchi
// CSC 369, Winter 2017
// Lab 7, Program 5: Putting it all together
// Outputs < word , word > that satisfy the specified conditions

// Conditions:
// 1. The two words appeared on the same line.
// 2. The first word is shorter than the second.
// 3. The first word appeared in multiple lines.
// 4. The second word is the longest word that co-occurred on the same line with the first word.

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

// Hash Set
import java.util.HashSet;
import java.util.ArrayList;

public class mixture {

    // Mapper Class
    // Emits < line number, (<word1>,<word2>) >
    public static class mixtureMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            // Remove leading white space
            String[] words = value.toString().replaceAll(" ", "").split(",");

            // Valid input
            if (words.length == 3) {
                // Get words
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                // Get word lengths
                int first = words[0].length();
                int second = words[1].length();
                int third = words[2].length();
                String lineNum = key.toString();

                // Conditions 1,2,4

                // One longest word
                // Longest word at front
                if (first > second && first > third) {
                    context.write(new Text(w2), new Text(lineNum + "," + w1));
                    context.write(new Text(w3), new Text(lineNum + "," + w1));
                }
                // Longest word in middle
                else if (second > first && second > third) {
                    context.write(new Text(w1), new Text(lineNum + "," + w2));
                    context.write(new Text(w3), new Text(lineNum + "," + w2));
                }
                // Longest word at end
                else if (third > first && third > second) {
                    context.write(new Text(w1), new Text(lineNum + "," + w3));
                    context.write(new Text(w2), new Text(lineNum + "," + w3));
                }

                // Two longest words
                // 1st and 2nd are the same length and longest
                if (first == second && first > third) {
                    context.write(new Text(w3), new Text(lineNum + "," + w1));
                    context.write(new Text(w3), new Text(lineNum + "," + w2));
                }
                // 1st and 3rd are the same length and longest
                else if (first == third && first > second) {
                    context.write(new Text(w2), new Text(lineNum + "," + w1));
                    context.write(new Text(w2), new Text(lineNum + "," + w3));
                }
                // 2nd and 3rd are same length and longest
                else if (second == third && second > first) {
                    context.write(new Text(w1), new Text(lineNum + "," + w2));
                    context.write(new Text(w1), new Text(lineNum + "," + w3));
                }
            }        
        }
    }

    // Reducer Class
    // Input: < line number, (<word1>,<word2>) >
    // Output: < word1, word2 >, where |word1| appeared on multiple lines
    public static class mixtureReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Long> lineNums = new HashSet<Long>();
            ArrayList<String> words = new ArrayList<String>();

            // Check if the key appeared on multiple lines
            for (Text val : values) {
                // Parse the current value
                String[] temp = val.toString().split(",");
                long lineIdx = Long.parseLong(temp[0]);
                String word = temp[1];

                // Add the line number to the set
                lineNums.add(lineIdx); 

                // Add the word to the array list
                words.add(word);              
            }


            // If so, output the words
            if (lineNums.size() > 1) {
                for (String word : words) {
                    context.write(key, new Text(word));
                }
            }
        }
    }

    // Section 4: MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: Get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: Register the MapReduce class
        job.setJarByClass(mixture.class);  

        // Step 3: Set I/O files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4: Register mapper and reducer
        job.setMapperClass(mixtureMapper.class);
        job.setReducerClass(mixtureReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Mixture - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
