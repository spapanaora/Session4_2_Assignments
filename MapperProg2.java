package acadgild.session4.task2;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperProg extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Pipe is a meta-character in regexp. so we would need to escape it
		//String[] lineArray = value.toString().split("\\|");
		String[] lineArray = value.toString().split(Pattern.quote("|"));
		System.out.println(lineArray[0]);
		System.out.println(lineArray[1]);
		// write key/value pair as (companyinfo, 1)
		if(!(lineArray[0].equals("NA") || lineArray[1].equals("NA")))
		{
			context.write(new Text(lineArray[0]), new IntWritable(1));
		}
	}
}
