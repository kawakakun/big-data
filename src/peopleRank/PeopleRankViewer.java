package peopleRank;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleRankViewer {
  public static class PageRankViewerMapper extends Mapper<Text, Text, FloatWritable, Text> {
    private FloatWritable peoplePr = new FloatWritable();
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    	  String line = value.toString();
          String[] tuple = line.split(",");
          float pr = Float.parseFloat(tuple[0]);
          peoplePr.set(pr);
          context.write(peoplePr, key);
    }
  }
  public static class DescFloatComparator extends FloatWritable.Comparator {
    // @Override
    public float compare(WritableComparator a,
        WritableComparable<FloatWritable> b) {
      return -super.compare(a, b);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      @SuppressWarnings("deprecation")
      Job job4_3 = new Job(conf, "PeopleRankViewer");
      job4_3.setJarByClass(PeopleRankViewer.class);
      job4_3.setInputFormatClass(KeyValueTextInputFormat.class);
      job4_3.setOutputKeyClass(FloatWritable.class);
      job4_3.setSortComparatorClass(DescFloatComparator.class);
      job4_3.setOutputValueClass(Text.class);
      job4_3.setMapperClass(PageRankViewerMapper.class);
      FileInputFormat.addInputPath(job4_3, new Path(args[0]));
      FileOutputFormat.setOutputPath(job4_3, new Path(args[1]));
      job4_3.waitForCompletion(true);
  }
}
