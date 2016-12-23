package peopleRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {
  public static class GraphBuilderMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    	StringBuilder out = new StringBuilder(); 
    	out.append("1.0,");
    	out.append(value.toString());
    	context.write(key,new Text(out.toString()));
    }
  }
  public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
		  context.write(key, value);
	  }
  }
  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  @SuppressWarnings("deprecation")
	  Job job4_1 = new Job(conf, "GraphBuilder");
	  job4_1.setInputFormatClass(KeyValueTextInputFormat.class);
	  job4_1.setJarByClass(GraphBuilder.class);
	  job4_1.setOutputKeyClass(Text.class);
	  job4_1.setOutputValueClass(Text.class);
	  job4_1.setMapperClass(GraphBuilderMapper.class);
	  job4_1.setReducerClass(GraphBuilderReducer.class);
	  FileInputFormat.addInputPath(job4_1, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job4_1, new Path(args[1]));
	  job4_1.waitForCompletion(true);
  }
}