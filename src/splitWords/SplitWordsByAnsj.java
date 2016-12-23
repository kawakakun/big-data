package splitWords;
//任务1 数据预处理的mapreduce程序
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
/*import java.util.Set;
import java.util.TreeSet;*/

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/

//@SuppressWarnings("deprecation")
public class SplitWordsByAnsj { 
	public static class SplitWordsByAnsjMapper extends Mapper<Object,Text,Text,Text> {
	//	private Set<String> peopleList;//人名列表
		private URI[] cacheFiles;//人名列表文件存储路径
		public void setup(Context context) throws IOException,InterruptedException {
		//	peopleList = new TreeSet<String>();
			cacheFiles = context.getCacheFiles();
			if(cacheFiles != null && cacheFiles.length > 0) {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while((line = br.readLine()) != null)	{
						StringTokenizer itr = new StringTokenizer(line);
						while(itr.hasMoreTokens())	{
							String name = itr.nextToken();
						//	peopleList.add(name);//将人名加入人名列表中
							UserDefineLibrary.insertWord(name, "usrdf", 1000);	//将人名加入到用户自定义词典中	
						}
					}
				}finally { br.close(); }
			}							
		}
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// default RecordReader: LineRecordReader; key: line offset; value: line string
			String line = value.toString();
			StringBuilder sbuffer = new StringBuilder();
			Result terms = DicAnalysis.parse(line);
			for(Term term:terms) {
				if(term.getNatureStr().equals("usrdf")) {
				//	if(peopleList.contains(term.getName())) {
						sbuffer.append(term.getName()+ "\t");
				//	}
				}
			}
			if(sbuffer.length() > 0) {
				Text word = new Text();
				word.set(sbuffer.toString());
				context.write(word,new Text(""));
			}
		}
		}
	/* public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
	     @SuppressWarnings("deprecation")
	     Job job1 = new Job(conf, "SplitWordsByAnsj"); 
	     job1.addCacheFile(new Path("people_name_list.txt").toUri());
	     //job1.setNumReduceTasks(Integer.parseInt("6"));//insert number of reducer
	     job1.setJarByClass(SplitWordsByAnsj.class); 
	     job1.setMapperClass(SplitWordsByAnsjMapper.class); 
	     job1.setMapOutputKeyClass(Text.class);
	     job1.setMapOutputValueClass(Text.class);
	     job1.setOutputKeyClass(Text.class); 
	     job1.setOutputValueClass(Text.class); 
	     FileInputFormat.addInputPath(job1, new Path(args[0])); 
	     FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	     job1.waitForCompletion(true);
	 }*/
}  
