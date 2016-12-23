package gephiProcess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GephiNormalProcess {
	 static final String prPath = "/home/caoermei/job4";
	 static final String lsPath = "/home/caoermei/job5";
	 static final String resultPath = "/home/caoermei/dian.csv";
	 static Map<String,String> map = new HashMap<String,String>();
	 public static void readFile() {
		 	String line = null;
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(lsPath));
				while((line = br.readLine()) != null)	{
					String[] item = line.split("\t");
					if(map.containsKey(item[1])) {
							map.put(item[1],map.get(item[1]) + "," + item[0]);
					}
					else {
							map.put(item[1],item[0]);
					}
				}
				br.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
			try {
				br = new BufferedReader(new FileReader(prPath));
				while((line = br.readLine()) != null)	{
					String[] item = line.split("\t");
					if(map.containsKey(item[1])) {
						map.put(item[1],map.get(item[1]) + "," + item[0]);
					}
					else {
						map.put(item[1],item[0]);
					}
				}
				br.close();
			}catch(Exception e) {
				e.printStackTrace();
			}			
	 }
	 public static void writeFile() {
			try {
				FileWriter out = new FileWriter(resultPath);
				out.write("Id,Label,Group,Weight\n");
				for (Map.Entry<String, String> entry : map.entrySet()) {  
					out.write(entry.getKey() + "," + entry.getKey()+"," + entry.getValue() + "\n"); 
				} 
				out.close();
			}catch(Exception e){
				e.printStackTrace();
			}
	}
	 public static void main(String[] args) throws IOException {
		 readFile();
		 writeFile();
		 System.out.println("点数据文件生成成功，请查看结果\n");
	 }
}
