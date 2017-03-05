// Holly Haraguchi
// CSC 369, Winter 2017
// Lab 7, Program 3: Grouping and Aggregation
// Outputs < item ID , <numberOfUnitsSold, profit> >

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

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class scores {

    // Mapper Class
    // Input format <itemId>, <numPurchased>, <pricePerUnit>, <shippingCost>
    //
    // Profit:
    //      If <= 100 units were sold --> profit = 2.25% of total revenue (item costs + shipping)
    //      If > 100 units were sold --> profit = 2.25% of total revenue on the first 100 items, then 3% of rest
    //
    // Split shipping costs proportionally
    public static class scoresMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            // Replace spaces with "" to allow for proper splitting 
            String noSpaces = value.toString().replaceAll("\\s", ""); 
            String[] vals = noSpaces.toString().split(",");

            if (vals.length == 4) {
                String id = vals[0];
                int numPurchased = Integer.parseInt(vals[1]);
                double ppu = Double.parseDouble(vals[2]);
                double shipping = Double.parseDouble(vals[3]);
                double profit = 0;

                if (numPurchased <= 100) {
                    profit = ((ppu * numPurchased) + shipping) * .0225;
                }
                else {
                    double shippingRate = shipping / numPurchased;
                    int remUnits = numPurchased - 100;

                    profit = ((ppu * 100) + (shippingRate * 100)) * .0225;
                    profit += ((ppu * remUnits) + (shippingRate * remUnits)) * .03;
                }

                String outVal = numPurchased + "," + String.format("%.2f", profit);
                context.write(new Text(id), new Text(outVal));
            }            
        }
    }

    // Reducer Class
    public static class scoresReducer extends Reducer< Text, Text, Text, Text> {
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
        job.setJarByClass(scores.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(scoresMapper.class);
        job.setReducerClass(scoresReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Scores - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
