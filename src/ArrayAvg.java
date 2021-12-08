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
        
public class ArrayAvg {
        
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
        Text text = new Text();
        MapWritable arr = new MapWritable();
        double count = 0;
        long sum = 0;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                sum += Long.parseLong(token);
                count++;
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            arr.put(new Text("1"), new DoubleWritable(count));
            arr.put(new Text("2"), new LongWritable(sum));
            context.write(text, arr);
        }
    }
        
    public static class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        double count = 0;
        long sum = 0;
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
            for (MapWritable arr: values) {
                count += ((DoubleWritable)arr.get(new Text("1"))).get();
                sum += ((LongWritable)arr.get(new Text("2"))).get();
            }
            context.write(new Text("avg"), new DoubleWritable(sum/count));
        }
    }
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
        Job job = new Job(conf, "arrayavg");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(ArrayAvg.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        job.waitForCompletion(true);
    }
}
