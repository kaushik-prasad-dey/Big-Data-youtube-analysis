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
 //Top5_categories class
 public class Top5_categories {
	//Mapper class
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text category = new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context )
		throws IOException, InterruptedException {
			String line = value.toString();
			String str[]=line.split("\t");
			if(str.length > 5){
				category.set(str[3]);
			}
		context.write(category, one);
	}
 }
 //Reducer class
    public static class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
		context.write(key, new IntWritable(sum));
 }
 }
 // this is my main class
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "youtube_analysis_top_5_categories");
    job.setJarByClass(Top5_categories.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	// mapper & reducer class
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
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