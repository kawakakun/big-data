package splitWords;
//任务 3 特征处理:人物关系图构建与特征归一化
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/


public class DataNormalization {
	public static class DataNormalizationMapper extends Mapper<Text, Text, Text, Text> {
	    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	          context.write(key, value);
	    }
	  }
	public static class NewPartitioner extends HashPartitioner<Text,Text> {  
	        public int getPartition(Text key, Text value, int numReduceTasks) {  
	            String term = new String();  
	            term = key.toString().split("#")[0]; 
	            return super.getPartition(new Text(term), value, numReduceTasks);  
	        }  
	    }
	 public static class DataNormalizationReducer extends Reducer<Text, Text, Text, Text> {
 	 private Text word1 = new Text();  
      private Text word2 = new Text();  
      String temp = new String();  
      static Text CurrentItem = new Text(" ");  
      static List<String> postingList = new ArrayList<String>();  

      public void reduce(Text key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {  
          int sum = 0;  
          temp = key.toString().split("#")[0];   
          word1.set(temp);  
            
          for(Text value : values) {
              sum += Integer.parseInt(value.toString());
          } 
          temp = key.toString().split("#")[1];
          word2.set(temp + ":" + sum);
          if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {  
              StringBuilder out = new StringBuilder();  
              long count = 0;
              long num = 0;
              double average = 0;
              Iterator<String> itr = postingList.iterator();
              for(;itr.hasNext();) {
             	 temp = itr.next(); 
                  count = count + Long.parseLong(temp.substring(temp.indexOf(":") + 1));  
              }
              Iterator<String> it = postingList.iterator();
              for(;it.hasNext();) {
             	 temp = it.next(); 
             	 num = Long.parseLong(temp.substring(temp.indexOf(":") + 1));
             	 average = (double)num/count;
             	 out.append(temp.substring(0,temp.indexOf(":") + 1) + String.format("%.4f",average));
             	 if (it.hasNext()) {
                      out.append(";");
                  }
              }
              if (out.length() > 0)  
                  context.write(CurrentItem, new Text(out.toString()));  
              postingList = new ArrayList<String>();  
          }  
          CurrentItem = new Text(word1);  
          postingList.add(word2.toString());  
      }
      public void cleanup(Context context) throws IOException, InterruptedException{
     	 StringBuilder out = new StringBuilder();  
          long count = 0;
          long num = 0;
          double average = 0;
          Iterator<String> itr = postingList.iterator();
          for(;itr.hasNext();) {
         	 temp = itr.next(); 
              count = count + Long.parseLong(temp.substring(temp.indexOf(":") + 1));  
          }
          Iterator<String> it = postingList.iterator();
          for(;it.hasNext();) {
         	 temp = it.next(); 
         	 num = Long.parseLong(temp.substring(temp.indexOf(":") + 1));
         	 average = (double)num/count;
         	 out.append(temp.substring(0,temp.indexOf(":") + 1) + String.format("%.4f",average));
         	 if (it.hasNext()) {
                  out.append(";");
              }
          }
          if (out.length() > 0)  
              context.write(CurrentItem, new Text(out.toString()));  
      }
 }
	/* public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
	     @SuppressWarnings("deprecation")
		Job job3 = new Job(conf, "DataNormalization"); 
	     job3.setJarByClass(DataNormalization.class); 
	     job3.setInputFormatClass(KeyValueTextInputFormat.class);
	     job3.setMapperClass(DataNormalizationMapper.class); 
	     job3.setMapOutputKeyClass(Text.class);
	     job3.setMapOutputValueClass(Text.class);
	     job3.setPartitionerClass(NewPartitioner.class);
	     job3.setReducerClass(DataNormalizationReducer.class);
	     job3.setOutputKeyClass(Text.class); 
	     job3.setOutputValueClass(Text.class); 
	     FileInputFormat.addInputPath(job3, new Path(args[0])); 
	     FileOutputFormat.setOutputPath(job3, new Path(args[1]));
	     System.exit(job3.waitForCompletion(true) ? 0 : 1);
	 }*/
}
