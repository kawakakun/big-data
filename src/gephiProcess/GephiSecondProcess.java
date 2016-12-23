package gephiProcess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class GephiSecondProcess {
	 static final String inPath = "/home/caoermei/job2";
	 static final String resultPath = "/home/caoermei/bian.csv";
	 public static void process() {
		 	String line = null;
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(inPath));
				FileWriter out = new FileWriter(resultPath);
				out.write("Source,Target,Weight\n");
				while((line = br.readLine()) != null)	{
					String[] item = line.split("\t");
					out.append(item[0].split("#")[0] + "," + item[0].split("#")[1]+ "," + item[1] + "\n");		
				}
				br.close();
				out.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
	}
	 public static void main(String[] args) throws IOException {
		 process();
		 System.out.println("边数据文件生成成功，请查看结果\n");
	 }
}
