package labelSpread;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelSpreadViewer {
	public static class LabelSpreadViewerMapper extends Mapper<Text,Text,Text,Text> {
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] name = key.toString().split(":");
			context.write(new Text(name[1]),new Text(name[0]));
		}
	}
	 public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  @SuppressWarnings("deprecation")
		  Job job5_3 = new Job(conf, "LableSpreadViewer");
		  FileInputFormat.addInputPath(job5_3, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job5_3, new Path(args[1]));
		  job5_3.setInputFormatClass(KeyValueTextInputFormat.class);
		  job5_3.setJarByClass(LabelSpreadViewer.class);
		  job5_3.setOutputKeyClass(Text.class);
		  job5_3.setOutputValueClass(Text.class);
		  job5_3.setMapperClass(LabelSpreadViewerMapper.class); 
		  job5_3.waitForCompletion(true);
	  }
}
