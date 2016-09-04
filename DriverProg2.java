package acadgild.session4.task2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DriverProg {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Creating configuration object from Hadoop configuration
		Configuration conf = new Configuration();
		
		//Creating JOB object from configuration
		Job job = new Job(conf, "Filter");
		job.setJarByClass(DriverProg.class);
		
		//Setting Mapper class and its output key/value types to job object
		job.setMapperClass(MapperProg.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Setting Reducer class and its output key/value types to job object
		job.setReducerClass(ReducerProg.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Setting input/output file format class to job object
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Setting input/output path to job object
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Delete the output directory if already exists
		FileSystem.get(conf).delete(new Path(args[1]));
		
		// call returns only when the job finishes either with success/failure
		job.waitForCompletion(true);
		
	}
}
