import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment6_meanTemp {
  public static class MeanTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      // variable denoting missing values
      public static final int MISSING = 9999; 

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          // get the line and convert to string
          String line = value.toString();
          // get the year from the line string
          String year = line.substring(15, 19);
          // get the month form the line string
          String month = line.substring(19, 21);
          // variable to hold the temperature
          int airTemperature;

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
 
  public static class MeanTemperatureReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    // double variable to hold the average result
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Double sum = 0.0d; // holds all the sums
      int count = 0; // holds the amount of appearance for the year

      // iterate through and add it to the sum and increment the count
      for(IntWritable value: values) {
        sum += value.get();
        count++;
      }
      
      // calculate the average
      Double mean = sum / count;
      // set it to the result variable
      result.set(mean);

      // write it <year, average>
      context.write(key, result);
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); // hadoop configuration

    Job job = Job.getInstance(conf, "Assignment6_Mean_Temperature"); // job name
    job.setJarByClass(Assignment6_meanTemp.class); // jar of the file
    job.setMapperClass(MeanTemperatureMapper.class); // set the mapper 
    job.setReducerClass(MeanTemperatureReducer.class); // set the reducer
    job.setOutputKeyClass(Text.class); // set the output key type
    job.setOutputValueClass(IntWritable.class); // set the output value type
    
    FileInputFormat.setInputDirRecursive(job, true); // recursively get all the files under the directory
    FileInputFormat.addInputPath(job, new Path(args[0])); // input path
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path
   
    System.exit(job.waitForCompletion(true) ? 0 : 1); // exit status code
  }

}


