import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment6_maxTemp {
  public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      public static final int MISSING = 9999; // variable denoting missing values

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString(); // get the line and convert to string
          String year = line.substring(15, 19); // get the year from the line string
          String month = line.substring(19, 21); // get the month from the line string
          int airTemperature; // variable to hold the temperature

          // if it is positive include the don't include the '+' character
          if(line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88,92));
          } else {
            airTemperature = Integer.parseInt(line.substring(87,92));
          }
          
          // quality of the data from the line string
          String quality = line.substring(92, 93);
          // check if it is not missing value and matches the quality standard
          if(airTemperature != MISSING && quality.matches("[01459]")) {
            // if so write it <year, temperature>
            context.write(new Text(year + "-" + month), new IntWritable(airTemperature));
          }
      }

  }
 
  public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int maxValue = Integer.MIN_VALUE; // min value (for the first one) for the maxValue variable to make sure you get the largest out of the two
      
      // iterate through the values and use the Math.max() function to get the max out of the two current values 
      // and set it to the local maxValue variable
      for(IntWritable value: values) {
        maxValue = Math.max(maxValue, value.get());
      }

      // write it <year, maxValue>
      context.write(key, new IntWritable(maxValue));
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); // hadoop configuartion

    Job job = Job.getInstance(conf, "Assignment6_Max_Temperature"); // job name
    job.setJarByClass(Assignment6_maxTemp.class); // jar of the file
    job.setMapperClass(MaxTemperatureMapper.class); // set the mapper
    job.setReducerClass(MaxTemperatureReducer.class); // set the reducer
    job.setOutputKeyClass(Text.class); // set the output key type
    job.setOutputValueClass(IntWritable.class); // set the output value type
    
    FileInputFormat.setInputDirRecursive(job, true); // recursively get all the files under the directory
    FileInputFormat.addInputPath(job, new Path(args[0])); // input path 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path 
  
    System.exit(job.waitForCompletion(true) ? 0 : 1); // exit status code
  }

}


