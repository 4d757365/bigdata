import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Assignment5_part2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private DoubleWritable salary = new DoubleWritable();
		private Text gender = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitText = value.toString().split("\t+");	
			gender.set(splitText[5]);
			
			try {
				double genderSalary = Double.parseDouble(splitText[2]);
				salary.set(genderSalary);
				context.write(gender, salary);
				
			} catch (NumberFormatException err) {
				System.out.println("NUMBER FORMAT ERR: " + err);
			}
		}
	}
	public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0d;
			int count = 0; 
			
			for(DoubleWritable value: values) {
				sum += value.get(); 
				count ++; 
			}
			double average = sum / count;
			result.set(average);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Assignment5_part2 (gender)");
		job.setJarByClass(Assignment5_part2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
