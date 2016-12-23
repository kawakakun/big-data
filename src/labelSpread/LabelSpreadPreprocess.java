package labelSpread;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelSpreadPreprocess {
	public static class LabelSpreadMapper extends Mapper<Text,Text,Text,Text> {
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Text keyInfo = new Text();
			Text valueInfo = new Text(); 
			StringBuilder out = new StringBuilder();  
			String label = "";
			double maxRate = 0.0;
			double rate = 0.0;
			String line = value.toString();
			String[] names = line.split(";");
			for (String name : names) {
				String[] people = name.split(":");
				rate = Double.parseDouble(people[1]);
				out.append(name + ":" + people[0] + ";");
				if(rate > maxRate) {
					maxRate = rate;
					label = people[0];
				}
		    }
			keyInfo.set(key.toString() + ":" + label);
			valueInfo.set(out.toString());
			context.write(keyInfo, valueInfo);
		}
	}
	 public static class LabelSpreadReducer extends Reducer<Text, Text, Text, Text> {
		  public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
			  context.write(key, value);
		  }
	  }
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  @SuppressWarnings("deprecation")
		  Job job5_1 = new Job(conf, "LableSpreadPreprocess");
		  job5_1.setInputFormatClass(KeyValueTextInputFormat.class);
		  job5_1.setJarByClass(LabelSpreadPreprocess.class);
		  job5_1.setOutputKeyClass(Text.class);
		  job5_1.setOutputValueClass(Text.class);
		  job5_1.setMapperClass(LabelSpreadMapper.class);
		  job5_1.setReducerClass(LabelSpreadReducer.class);
		  FileInputFormat.addInputPath(job5_1, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job5_1, new Path(args[1]));
		  job5_1.waitForCompletion(true);
	  }
}
