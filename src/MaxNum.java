import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class MaxNum {
        
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        long max = Long.MIN_VALUE;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                long parsed_num = Long.parseLong(token);
                if (parsed_num > max) {
                    max = parsed_num;
                }   
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("1"), new LongWritable(max));
        }
    }
        
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException {
            long max = Long.MIN_VALUE;
            for (LongWritable val : values) {
                if (val.get() > max) {
                    max = val.get();
                }
            }
            context.write(key, new LongWritable(max));
        }
    }
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
        Job job = new Job(conf, "arraysum");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(WordCount.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        job.waitForCompletion(true);
    }
}
