import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SumFileSize {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SumFileSize.do1(args);
	}
	public static void do2(String[] args) throws IOException{
		String uri = "hdfs://127.0.0.1:9000/user/";
		Configuration conf = new Configuration();
		FileSystem fs  = FileSystem.get(URI.create(uri),conf);
		Path[] paths = new Path[args.length];
		for(int i = 0; i < args.length; i++){
			paths[i] = new Path(uri+"youli/"+args[i]); 
		}
		double size = 0L;
		FileStatus[] fileStatus = fs.listStatus(paths);
		for(FileStatus fstatus : fileStatus){
			if(fstatus.isDir())
				continue;
			size += fstatus.getLen();
		}
		System.out.println(fileStatus.length+"个文件");
		System.out.println("共"+size/(1024*1024)+"MB");
	}
	public static void do1(String[] args) throws IOException{
		String uri = "hdfs://172.19.0.122:19000/user/";
		Configuration conf = new Configuration();
		FileSystem fs  = FileSystem.get(URI.create(uri),conf);
		Path[] paths = new Path[args.length];
		for(int i = 0; i < args.length; i++){
			paths[i] = new Path(uri+"dic/"+args[i]); 
		}
		double size = 0L;
		FileStatus[] fileStatus = fs.listStatus(paths);
		for(FileStatus fstatus : fileStatus){
			if(fstatus.isDir())
				continue;
			size += fstatus.getLen();
		}
		System.out.println(fileStatus.length+"个文件");
		System.out.println("共"+size/(1024*1024)+"MB");
	}
}
