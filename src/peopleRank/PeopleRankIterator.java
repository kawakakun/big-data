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

public class PeopleRankIterator {
  private static final double damping = 0.85;

  public static class PRIterMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tuple = line.split(",");
      double pr = Double.parseDouble(tuple[0]);

      if (tuple.length > 1) {
        String[] names = tuple[1].split(";");
        for (String name : names) {
        	String[] people = name.split(":");
        	String prValue = String.valueOf(pr * Double.parseDouble(people[1]));
            context.write(new Text(people[0]), new Text(prValue));
        }
        context.write(key, new Text("|" + tuple[1]));
      }
    }
  }

  public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String links = "";
      double namerank = 0;
      for (Text value : values) {
        String tmp = value.toString();

        if (tmp.startsWith("|")) {
          links = "," + tmp.substring(tmp.indexOf("|") + 1);// index从0开始
          continue;
        }
        namerank += Double.parseDouble(tmp);
      }
      namerank = (double) (1 - damping) + damping * namerank; // PeopleRank的计算迭代公式
      context.write(new Text(key), new Text(String.valueOf(namerank) + links));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    @SuppressWarnings("deprecation")
	Job job4_2 = new Job(conf, "PeopleRankIterator");
    job4_2.setJarByClass(PeopleRankIterator.class);
    job4_2.setInputFormatClass(KeyValueTextInputFormat.class);
    job4_2.setOutputKeyClass(Text.class);
    job4_2.setOutputValueClass(Text.class);
    job4_2.setMapperClass(PRIterMapper.class);
    job4_2.setReducerClass(PRIterReducer.class);
    FileInputFormat.addInputPath(job4_2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job4_2, new Path(args[1]));
    job4_2.waitForCompletion(true);
  }
}
