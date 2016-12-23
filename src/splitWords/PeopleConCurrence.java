package splitWords;
//任务 2 特征抽取:人物同现统计
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/

public class PeopleConCurrence {
	public static class PeopleConCurrenceMapper extends Mapper<Object,Text,Text,Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// default RecordReader: LineRecordReader; key: line offset; value: line string
			Text keyInfo = new Text();
			Text valueInfo = new Text();
			String line = value.toString();
			String[] list = line.split("\t");
			List<String> peopleList = new ArrayList<String>();
			int n = list.length;
			for(int i = 0; i < n; i++){
				if(!peopleList.contains(list[i])) {
					peopleList.add(list[i]);
				}
			}
			n = peopleList.size();
			for(int i = 0; i < n; i++){
				String A = peopleList.get(i);
				for(int j = 0; j < n; j++){
					if(i != j) {
						String B = peopleList.get(j);
						keyInfo.set(A + "#" + B);
						valueInfo.set("1");
			            context.write(keyInfo, valueInfo);
					}
				}
			}
		}
	}
	public static class PeopleConCurrenceReducer extends Reducer<Text,Text, Text, Text> {
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
	/*public static void main(String[] args) throws Exception {
	      Configuration conf = new Configuration();
	      @SuppressWarnings("deprecation")
	      Job job2 = new Job(conf, "PeopleConCurrence");
	      job2.setJarByClass(PeopleConCurrence.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);
	      job2.setMapperClass(PeopleConCurrenceMapper.class);
	    //  job2.setCombinerClass(PeopleConCurrenceReducer.class);
	      job2.setReducerClass(PeopleConCurrenceReducer.class);
	      FileInputFormat.addInputPath(job2, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	      job2.waitForCompletion(true);
	  }*/
}
