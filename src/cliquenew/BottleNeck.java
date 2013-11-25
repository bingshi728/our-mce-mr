package cliquenew;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BottleNeck {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: CliqueMain <in> <out> <reducenum>");
	      System.exit(2);
	    }
		String in=args[0];
		String pre=args[1];
		int reducenum=Integer.valueOf(args[2]);
		
		
		Job job = new Job(conf,"bottle neck clique");		
		
		job.setJarByClass(BottleNeck.class);
		job.setMapperClass(bottleneck.BottleneckMapper.class);
		job.setPartitionerClass(bottleneck.BottleneckPartitioner.class);
		job.setReducerClass(bottleneck.BottleneckTimeAllReducer.DetectReducer.class);//换Reducer
		
		job.setMapOutputKeyClass(bottleneck.PairTypeInt.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(pre+"_result"));
		
		long t1 = System.currentTimeMillis();
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		System.out.println("0-phase cost:"+ (t2-t1));
		
	}

}
