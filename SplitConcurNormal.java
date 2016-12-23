package splitWords;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

@SuppressWarnings("deprecation")
public class SplitConcurNormal { 
	public static class SplitConcurNormalMapper extends Mapper<Object,Text,Text,Text> {
		private Set<String> peopleList;//人名列表
		private URI[] cacheFiles;//人名列表文件存储路径
		public void setup(Context context) throws IOException,InterruptedException {
			peopleList = new TreeSet<String>();
			cacheFiles = context.getCacheFiles();
			if(cacheFiles != null && cacheFiles.length > 0) {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while((line = br.readLine()) != null)	{
						StringTokenizer itr = new StringTokenizer(line);
						while(itr.hasMoreTokens())	{
							String name = itr.nextToken();
							peopleList.add(name);//将人名加入人名列表中
							UserDefineLibrary.insertWord(name, "userdefine", 1000);	//将人名加入到用户自定义词典中	
						}
					}
				}finally { br.close(); }
			}							
		}
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// default RecordReader: LineRecordReader; key: line offset; value: line string
			Text keyInfo = new Text();
			Text valueInfo = new Text();
			List<String> list = new ArrayList<String>();
			String line = value.toString();
			//Result res = DicAnalysis(line);
			Result terms = DicAnalysis.parse(line);
			for(Term term:terms) {
				if(term.getNatureStr().equals("userdefine")) {
					if(peopleList.contains(term.getName()) && !list.contains(term.getName())) {
						list.add(term.getName());
					}
				}
			}
			int n = list.size();
			for(int i = 0; i < n; i++){
				String A = list.get(i);
				for(int j = 0; j < n; j++){
					if(i != j) {
						String B = list.get(j);
						keyInfo.set(A + "#" + B);
						valueInfo.set("1");
			            context.write(keyInfo, valueInfo);
					}
				}
			}
		}
	}
	public static class SplitConcurNormalCombiner extends Reducer<Text,Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            result.set(new IntWritable(sum).toString());
            context.write(key, result);
        }
	}
	 public static class NewPartitioner extends HashPartitioner<Text,Text> {  
	        public int getPartition(Text key, Text value, int numReduceTasks) {  
	            String term = new String();  
	            term = key.toString().split("#")[0]; 
	            return super.getPartition(new Text(term), value, numReduceTasks);  
	        }  
	    }
	 public static class SplitConcurNormalReducer extends Reducer<Text, Text, Text, Text> {
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
	 public static void main(String[] args) {
		 try {
			Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if(otherArgs.length != 2) {
	            System.err.println("Usage: SplitWordsByAnsj <in> <out>");
	            System.exit(2);
	        }
	        Job job1 = new Job(conf, "job1"); 
	        job1.addCacheFile(new Path("people_name_list.txt").toUri());
			//job1.setNumReduceTasks(Integer.parseInt("6"));//insert number of reducer
			job1.setJarByClass(SplitConcurNormal.class); 
			job1.setMapperClass(SplitConcurNormalMapper.class); 
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setCombinerClass(SplitConcurNormalCombiner.class);
			job1.setPartitionerClass(NewPartitioner.class);
			job1.setReducerClass(SplitConcurNormalReducer.class);
			job1.setOutputKeyClass(Text.class); 
			job1.setOutputValueClass(Text.class); 
			FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); 
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
	    }
		 catch (Exception e) { e.printStackTrace(); }
	 }
}  