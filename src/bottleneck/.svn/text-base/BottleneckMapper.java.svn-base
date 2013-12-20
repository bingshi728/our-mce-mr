package bottleneck;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BottleneckMapper extends
		Mapper<LongWritable, Text, PairTypeInt, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		int k = -1;
		
		String[] kvoriginal = str.split("\\t");
		k = Integer.parseInt(kvoriginal[0]);
		if (k == 0)
			return;
		StringTokenizer stk = new StringTokenizer(kvoriginal[1]);
		if (k == -1)// 子图信息
		{
			int type = Integer.valueOf(stk.nextToken("@"));
			int p = Integer.valueOf(stk.nextToken("@"));
			int tk = Integer.valueOf(stk.nextToken("@"));
			context.write(new PairTypeInt(type, p, tk),
					new Text(stk.nextToken("toend").substring(1)));
		} else// 边连接信息
		{
			int type = Integer.valueOf(stk.nextToken("#"));
			String[] parts = stk.nextToken("#").split(",");
			int tk = Integer.valueOf(stk.nextToken("#"));
			String toend = stk.nextToken("toend").substring(1);

			for (String i : parts)
				context.write(new PairTypeInt(1, Integer.parseInt(i.trim()), tk), new Text(toend));
		}

	}

}
