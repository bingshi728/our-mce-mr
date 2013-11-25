package bottleneck;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BottleneckPartitioner extends Partitioner<PairTypeInt,Text> {

	@Override
	public int getPartition(PairTypeInt key, Text value, int num) {
		return (key.getB())%num;//平均分配到各个计算节点之上
	}

}
