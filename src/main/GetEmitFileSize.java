package main;
import java.io.File;


public class GetEmitFileSize {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File path = new File("/home/"+RunOver.usr+"/CliqueHadoop/outresult/"+args[0]);
		File[] fs = path.listFiles();
		long size = 0;
		for(File f:fs){
			size+=f.length();
		}
		System.out.println("Emit File size="+size);
	}

}
