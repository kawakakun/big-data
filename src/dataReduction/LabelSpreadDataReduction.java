package dataReduction;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/


public class LabelSpreadDataReduction {
	public static class LabelSpreadDataReductionMapper extends Mapper<Text,Text,Text,Text> {
		private MultipleOutputs<Text,Text> mos;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void setup(Context context) throws IOException,InterruptedException {
			mos = new MultipleOutputs(context);
		}
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			mos.write(key,value,key.toString());
		}
		public void cleanup(Context context) throws IOException,InterruptedException {
			mos.close();
		}
	}
	/* public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  @SuppressWarnings("deprecation")
		  Job job6_2 = new Job(conf, "LabelSpreadDataReduction");
		  FileInputFormat.addInputPath(job6_2, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job6_2, new Path(args[1]));
		  job6_2.setInputFormatClass(KeyValueTextInputFormat.class);
		  job6_2.setJarByClass(LabelSpreadDataReduction.class);
		  job6_2.setOutputKeyClass(Text.class);
		  job6_2.setOutputValueClass(Text.class);
		  job6_2.setMapperClass(LabelSpreadDataReductionMapper.class); 
		  job6_2.waitForCompletion(true);
	  }*/

}
 