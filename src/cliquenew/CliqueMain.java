 package cliquenew;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import detect.DetectTimeAll;



public class CliqueMain {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 3) {
	      System.err.println("Usage: CliqueMain <in> <out> <reducenum>");
	      System.exit(2);
	    }
	  //String in=args[0];
	    int arglen = args.length;
		String pre=args[arglen -2];
		int reducenum=Integer.valueOf(args[arglen-1]);

		
		Job job = new Job(conf,"detect clique");		
		
		job.setJarByClass(CliqueMain.class);
		job.setMapperClass(DetectTimeAll.DetectMapper.class);
		job.setReducerClass(DetectTimeAll.DetectReducer.class);//换Reducer
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		for(int i = 0; i < arglen -2;i++)
			FileInputFormat.addInputPath(job, new Path(args[i]));
		FileOutputFormat.setOutputPath(job, new Path(pre+"_result_Binary"));
		
		long t1 = System.currentTimeMillis();
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		System.out.println("0-phase cost:"+ (t2-t1));
		
		
		/*long curVal = job.getCounters().findCounter(DetectTimeAll.LoadBalance.Balance).getValue(); 
		
		while(curVal!=0)
		{
			count++;
			long t1 = System.currentTimeMillis();
			Job jobBalance = new Job(conf,"bottleneck procedure");		
			jobBalance.setJarByClass(CliqueMain.class);
			jobBalance.setNumReduceTasks(reducenum);
			//FileInputFormat.addInputPath(jobBalance, new Path(pre+"_result"+(count-1)));
			//FileOutputFormat.setOutputPath(jobBalance, new Path(pre+"_result"+count));
			FileInputFormat.addInputPath(jobBalance, new Path(pre+"_result"));
			FileOutputFormat.setOutputPath(jobBalance, new Path(pre+"_result"+count));
			jobBalance.setMapperClass(BottleneckMapper.class);
			jobBalance.setPartitionerClass(BottleneckPartitioner.class);
			jobBalance.setReducerClass(BottleneckTimeAllReducer.class);//换Reducer
			jobBalance.setMapOutputKeyClass(PairTypeInt.class);
			jobBalance.setMapOutputValueClass(Text.class);
			jobBalance.setOutputKeyClass(IntWritable.class);
			jobBalance.setOutputValueClass(Text.class);
			jobBalance.waitForCompletion(true);
			long t2 = System.currentTimeMillis();
			System.out.println(count + "-phase cost:"+(t2-t1));
			//curVal = jobBalance.getCounters().findCounter(BottleneckTimeAllReducer.LoadBalance.Balance).getValue();
		//}	
		//*/	
	}

}
