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
        
public class Sort {
        
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                context.write(new IntWritable(Integer.parseInt(token)), new IntWritable(1));
            }
        }
    }
        
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
            for (IntWritable val: values) {
                context.write(key, new IntWritable(1));
            }
        }
    }
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
            Job job = new Job(conf, "Sort");
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
            
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
