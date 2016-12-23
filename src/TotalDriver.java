import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import dataReduction.LabelSpreadDataReduction;
import dataReduction.LabelSpreadDataReduction.LabelSpreadDataReductionMapper;
import labelSpread.LabelSpreadPreprocess;
import labelSpread.LabelSpreadProcess;
import labelSpread.LabelSpreadViewer;
import peopleRank.GraphBuilder;
import peopleRank.PeopleRankIterator;
import peopleRank.PeopleRankViewer;
import splitWords.DataNormalization;
import splitWords.DataNormalization.DataNormalizationMapper;
import splitWords.DataNormalization.DataNormalizationReducer;
import splitWords.DataNormalization.NewPartitioner;
import splitWords.PeopleConCurrence;
import splitWords.PeopleConCurrence.PeopleConCurrenceMapper;
import splitWords.PeopleConCurrence.PeopleConCurrenceReducer;
import splitWords.SplitWordsByAnsj;
import splitWords.SplitWordsByAnsj.SplitWordsByAnsjMapper;
/*import splitWords.SplitConcurNormal;
import splitWords.SplitConcurNormal.NewPartitioner;
import splitWords.SplitConcurNormal.SplitConcurNormalCombiner;
import splitWords.SplitConcurNormal.SplitConcurNormalMapper;
import splitWords.SplitConcurNormal.SplitConcurNormalReducer;*/

public class TotalDriver {
	private static int times = 10; // 设置迭代次数
	private static long startTime = 0;
	private static long endTime  = 0;
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 7) {
				System.err.println("Usage : input job1out job2out job3out job4out job5out job6out " + otherArgs.length);
				System.exit(7);
			}
			FileSystem fs = FileSystem.get(URI.create("run_time.txt"),conf);
			FSDataOutputStream out = fs.create(new Path("run_time.txt"),true);
		/*	startTime = System.currentTimeMillis();
			Job job1 = new Job(conf, "job1"); 
			job1.addCacheFile(new Path("people_name_list.txt").toUri());
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
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
			job1.waitForCompletion(true);
		    endTime = System.currentTimeMillis();
		    out.write(("SplitConCurNormal:\t" + String.valueOf(endTime - startTime) + "ms\n").getBytes("UTF-8"));*/
			startTime = System.currentTimeMillis();
			Job job1 = new Job(conf, "SplitWordsByAnsj"); 
		    job1.addCacheFile(new Path("people_name_list.txt").toUri());
		    //job1.setNumReduceTasks(Integer.parseInt("6"));//insert number of reducer
		    job1.setJarByClass(SplitWordsByAnsj.class); 
		    job1.setMapperClass(SplitWordsByAnsjMapper.class); 
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class); 
		    job1.setOutputValueClass(Text.class); 
		    FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); 
		    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		    job1.waitForCompletion(true);
		    endTime = System.currentTimeMillis();
		    out.write(("SplitWordsByAnsj:\t" + String.valueOf(endTime - startTime) + "ms\n").getBytes("UTF-8"));
		    
		    startTime = System.currentTimeMillis();
		    Job job2 = new Job(conf, "PeopleConCurrence");
		    job2.setJarByClass(PeopleConCurrence.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    job2.setMapperClass(PeopleConCurrenceMapper.class);
		    job2.setCombinerClass(PeopleConCurrenceReducer.class);
		    job2.setReducerClass(PeopleConCurrenceReducer.class);
		    FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		    job2.waitForCompletion(true);
		    endTime = System.currentTimeMillis();
		    out.write(("PeopleConcurrence:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    
		    startTime = System.currentTimeMillis();
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
		    FileInputFormat.addInputPath(job3, new Path(otherArgs[2])); 
		    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
		    job3.waitForCompletion(true);
		    endTime = System.currentTimeMillis();
		    out.write(("DataNormalization:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		   
		    String[] forGB = { "", otherArgs[4] + "/Data0" };
		    forGB[0] = otherArgs[3];
		    startTime = System.currentTimeMillis();
		    GraphBuilder.main(forGB);
		    endTime = System.currentTimeMillis();
		    out.write(("GraphBuilder:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    

		    String[] forItr = { "", "" };
		    for (int i = 0; i < times; i++) {
		      forItr[0] = otherArgs[4] + "/Data" + i;
		      forItr[1] = otherArgs[4] + "/Data" + String.valueOf(i + 1);
		      startTime = System.currentTimeMillis();
		      PeopleRankIterator.main(forItr);
		      endTime = System.currentTimeMillis();
			  out.write(("PeopleRankIterator:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    }
		    
		    String[] forRV = { otherArgs[4] + "/Data" + times, otherArgs[4] + "/FinalRank" };
		    startTime = System.currentTimeMillis();
		    PeopleRankViewer.main(forRV);
		    endTime = System.currentTimeMillis();
		    out.write(("PeopleRankViewer:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    
		    String[] forLS = { "", otherArgs[5] + "/Label0" };
		    forLS[0] = otherArgs[3];
		    startTime = System.currentTimeMillis();
		    LabelSpreadPreprocess.main(forLS);
		    endTime = System.currentTimeMillis();
		    out.write(("LabelSpreadPreprocess:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));

		    String[] forLSP = { "", "" };
		    for (int i = 0; i < times; i++) {
		      forLSP[0] = otherArgs[5] + "/Label" + i;
		      forLSP[1] = otherArgs[5] + "/Label" + String.valueOf(i + 1);
		      startTime = System.currentTimeMillis();
		      LabelSpreadProcess.main(forLSP);
		      endTime = System.currentTimeMillis();
			  out.write(("LabelSpreadProcess:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    }

		    String[] forLSV = { otherArgs[5] + "/Label" + times, otherArgs[5] + "/FinalResult" };
		    startTime = System.currentTimeMillis();
		    LabelSpreadViewer.main(forLSV);
		    endTime = System.currentTimeMillis();
		    out.write(("LabelSpreadViewer:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    
		    startTime = System.currentTimeMillis();
		    Job job6_2 = new Job(conf, "LabelSpreadDataReduction");
		    FileInputFormat.addInputPath(job6_2, new Path(otherArgs[5] + "/FinalResult"));
		    FileOutputFormat.setOutputPath(job6_2, new Path(otherArgs[6]));
		    job6_2.setInputFormatClass(KeyValueTextInputFormat.class);
		    job6_2.setJarByClass(LabelSpreadDataReduction.class);
		    job6_2.setOutputKeyClass(Text.class);
		    job6_2.setOutputValueClass(Text.class);
		    job6_2.setMapperClass(LabelSpreadDataReductionMapper.class); 
		    job6_2.waitForCompletion(true);
		    endTime = System.currentTimeMillis();
		    out.write(("LabelSpreadDataReduction:\t" + String.valueOf((endTime - startTime)) + "ms\n").getBytes("UTF-8"));
		    out.close();
		}
		catch (Exception e) { e.printStackTrace(); }
		}
}
