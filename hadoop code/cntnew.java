import java.io.IOException; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//user defined counters

public class cntnew{

	public enum ct
	  {
		  cnt,nt
	  };

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{


public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] line=value.toString().split(",");
	int i=Integer.parseInt(line[10]);//quantity value
	float sales=Float.parseFloat(line[9]);//sales value
				
				if(sales>=500.0)
		          {
		       context.getCounter(ct.cnt).increment(1);

		   Text tt=new Text(line[3]);
			context.write(tt,new IntWritable(i));

		          }
		          if(i > 5)
		          {
				      context.getCounter(ct.nt).increment(1);
				         }
		        	   }
		  } 

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	
	public void reduce(Text key, IntWritable values, Context context) throws 
	IOException, InterruptedException
	{
		context.write(key, values);
	}
}
public static void main(String[] args) throws Exception 
{
	Configuration conf = new Configuration();
	Job job = new Job(conf, "ecommerce.csv");
	job.setJarByClass(cntnew.class);
	job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
  job.setInputFormatClass(TextInputFormat.class);	    
  job.setOutputFormatClass(TextOutputFormat.class);    
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
	Counters cn=job.getCounters();
	cn.findCounter(ct.cnt).getValue();
   cn.findCounter(ct.nt).getValue();
}        
	}