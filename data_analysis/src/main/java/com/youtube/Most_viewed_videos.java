package com.youtube;

//import the necessary libraries
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Most_viewed_videos {
    // Most Viewed Mapper
    public static class MostViewedMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text vidid = new Text();
        private IntWritable numview = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            if (str.length > 5) {
                vidid.set(str[0]);
                if (str[5].matches("\\d+.+")) {
                    int f = (int) Float.parseFloat(str[5]);
                    numview.set(f);
                }
            }
            context.write(vidid, numview);
        }
    }
    // Most Viewed Reducer
    public static class MostViewedReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            int l = 0;
            for (IntWritable val : values) {
                l += 1; // counts number of values are there for that key
                sum += val.get();
            }
            sum = sum / l; // takes the average of the sum
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Most_Viewed_video");
        job.setJarByClass(Most_viewed_videos.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //set Mapper Name
        job.setMapperClass(MostViewedMap.class);
        //set Reducer Name
        job.setReducerClass(MostViewedReduce.class);
        // input format & output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // input & output location
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //exit from that jar after completion
        System.exit(job.waitForCompletion(true) ? 0:1);
        }

}
