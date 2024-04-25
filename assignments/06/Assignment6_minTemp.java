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

public class Assignment6_minTemp {
  public static class MinTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      public static final int MISSING = 9999;  // variable denoting missing values

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString();  // get the line and convert to string
          String year = line.substring(15, 19);  // get the year from the line string
          String month = line.substring(19, 21); // get the month from the line string
          int airTemperature; // variable to hold the temperature
          
          // if it is positive include the don't include the '+' character
          if(line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88,92));
          } else {
             // else include the '-' character to show it is negative
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
 
  public static class MinTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int minValue = Integer.MAX_VALUE; // set the max value (default for the first) to make sure you get the minimum out of the two values
      
      // iterature through the values and compare using Math.min() to get the min out the two current values 
      for(IntWritable value: values) {
        minValue = Math.min(minValue, value.get());
      }

      // write it <year, minValue>
      context.write(key, new IntWritable(minValue));
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); // hadoop configuration

    Job job = Job.getInstance(conf, "Assignment6_Min_Temperature"); // job name
    job.setJarByClass(Assignment6_minTemp.class); // jar of the file
    job.setMapperClass(MinTemperatureMapper.class); // set the mapper
    job.setReducerClass(MinTemperatureReducer.class); // set the reducer
    job.setOutputKeyClass(Text.class); // set the output key type
    job.setOutputValueClass(IntWritable.class); // set the output value type
    
    FileInputFormat.setInputDirRecursive(job, true);  // recursively get all the files under the directory
    FileInputFormat.addInputPath(job, new Path(args[0])); // input path 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path 
    
    System.exit(job.waitForCompletion(true) ? 0 : 1); // exit status code
  }

}


