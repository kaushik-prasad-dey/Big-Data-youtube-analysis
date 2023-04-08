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

public class Top10_rated_videos {
    // create the top 10 rated video mapper
    public static class Top10VideoMap extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text video_name = new Text();
        private FloatWritable rating = new FloatWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            if (str.length > 7) {
                video_name.set(str[0]);
                if (str[6].matches("\\d+(\\.\\d{1,3})?")) {
                    float f = Float.parseFloat(str[6]); // type casting string to float
                    rating.set(f);
                }
            }
            context.write(video_name, rating);
        }
    }

    // top 10 video reducer
    public static class Top10VideoReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int l = 0;
            for (FloatWritable val : values) {
                l += 1;
                sum += val.get();
            }
            sum = sum / l;
            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Top_10_video_rating");
        job.setJarByClass(Top10_rated_videos.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        //set Mapper Class
        job.setMapperClass(Top10VideoMap.class);
        //set Reducer Class
        job.setReducerClass(Top10VideoReduce.class);
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
