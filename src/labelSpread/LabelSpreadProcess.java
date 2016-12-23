package labelSpread;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelSpreadProcess {
	public static class LabelSpread2Mapper extends Mapper<Text,Text,Text,Text> {
			protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
				Map<String,Double> nameMap = new HashMap<String,Double>();
				Text keyInfo = new Text();
				Text valueInfo = new Text(); 
				String label = "";
				double maxRate = 0.0;
				double rate = 0.0;
				String line = value.toString();
				String[] names = line.split(";");
				for (String name : names) {
					String[] people = name.split(":");
					rate = Double.parseDouble(people[1]);
					if(nameMap.containsKey(people[2])) {
						nameMap.put(people[2],nameMap.get(people[2]) + rate);
						rate = nameMap.get(people[2]) + rate;
					}
					else {
						nameMap.put(people[2], rate);
					}
					if(rate > maxRate) {
						maxRate = rate;
						label = people[2];
					}
			    }
				keyInfo.set(key.toString().split(":")[0] + ":" + label);
				for(String name:names) {
					String[] people = name.split(":");
					context.write(new Text(people[0]), keyInfo);	
				}
				valueInfo.set("|"+ label + "#"+ line);
				context.write(new Text(key.toString().split(":")[0]), valueInfo);
			}
		}
	 public static class LabelSpread2Reducer extends Reducer<Text, Text, Text, Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			  Text keyInfo = new Text();
			  Text valueInfo = new Text(); 
			  String namelist = "";
			  String word = "";
			  StringBuilder out = new StringBuilder();
			  Map<String,String> nameMap = new HashMap<String,String>();
			  for (Text value : values) {
		        String tmp = value.toString();
		        if (tmp.startsWith("|")) {
		          namelist = tmp.substring(tmp.indexOf("#") + 1);// index从0开始
		          word = tmp.substring(tmp.indexOf("|") + 1, tmp.indexOf("#"));
		          continue;
		        }
		        String[] people = tmp.split(":");
		        nameMap.put(people[0],people[1]);
		      }
			  keyInfo.set(key.toString() + ":" + word);
			  String[] names = namelist.split(";");
			  try {
				  for (String name : names) {
					  String[] people = name.split(":");
						if(nameMap.containsKey(people[0])) {
							out.append(people[0] + ":" + people[1] + ":" + nameMap.get(people[0]) + ";");
						}
						else {
							throw new Exception();
						}	 
				  }		 
		  } catch(Exception e) { System.out.println("error in LabelSpread2.reduce");};
		  valueInfo.set(out.toString());
		  context.write(keyInfo, valueInfo);
	  }
	 }
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  @SuppressWarnings("deprecation")
		  Job job5_2 = new Job(conf, "LabelSpreadProcess");
		  job5_2.setInputFormatClass(KeyValueTextInputFormat.class);
		  job5_2.setJarByClass(LabelSpreadProcess.class);
		  job5_2.setOutputKeyClass(Text.class);
		  job5_2.setOutputValueClass(Text.class);
		  job5_2.setMapperClass(LabelSpread2Mapper.class);
		  job5_2.setReducerClass(LabelSpread2Reducer.class);
		  FileInputFormat.addInputPath(job5_2, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job5_2, new Path(args[1]));
		  job5_2.waitForCompletion(true);
	  }
}