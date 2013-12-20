import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cliquenew.BottleNeck;
import cliquenew.CliqueMain;
import detect.DetectTimeAll;

public class RunOver {

	int arglen;
	int pre = 0;
	int reducenum;

	/**
	 * @param args
	 */
	public void doStep1(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "detect clique");

		job.setJarByClass(CliqueMain.class);
		job.setMapperClass(DetectTimeAll.DetectMapper.class);
		job.setReducerClass(DetectTimeAll.DetectReducer.class);// 换Reducer
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		for (int i = 0; i < arglen - 1; i++)
			FileInputFormat.addInputPath(job, new Path(args[i]));
		FileOutputFormat.setOutputPath(job, new Path(pre + "_result_binary"));

		long t1 = System.currentTimeMillis();
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		System.out.println(pre + "-phase cost:" + (t2 - t1));

	}

	public void doStep2(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		String in = "binaryinputR";

		Job job = new Job(conf, "bottle neck clique");

		job.setJarByClass(BottleNeck.class);
		job.setMapperClass(bottleneck.BottleneckMapper.class);
		job.setPartitionerClass(bottleneck.BottleneckPartitioner.class);
		job.setReducerClass(bottleneck.BottleneckTimeAllReducer.DetectReducer.class);// 换Reducer

		job.setMapOutputKeyClass(bottleneck.PairTypeInt.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(pre + "_result_binary"));

		long t1 = System.currentTimeMillis();
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		System.out.println(pre + "-phase cost:" + (t2 - t1));
	}

	public void dojob(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: CliqueMain <in> <reducenum>");
			System.exit(2);
		}
		// String in=args[0];
		arglen = args.length;
		pre = 0;
		reducenum = Integer.valueOf(args[arglen - 1]);

		doStep1(args);
		synchronized (this) {
			while (((long) RemoteSSH.getRemoteFilesSize()) != 0) {
				Process p = Runtime
						.getRuntime()
						.exec(new String[] { "/bin/sh", "-c",
								"/home/dic/hadoop-1.1.2/bin/hadoop fs -rmr binaryinputR/" });
				p.waitFor();
				p.destroy();
				RemoteSSH.batch();
				this.wait(10000);
				pre++;
				doStep2(args);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		long t1 = System.currentTimeMillis();
		new RunOver().dojob(args);
		long t2 = System.currentTimeMillis();
		System.out.println("all:" + (t2 - t1));
	}

}
