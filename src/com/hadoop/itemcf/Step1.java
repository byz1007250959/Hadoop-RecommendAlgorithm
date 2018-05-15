package com.hadoop.itemcf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IDEA
 * USER: DUAN
 * DATE: 2018/5/14.
 */
public class Step1 {
    public static class Step1Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[0]));
            Text v=new Text(tokens[1]);
            context.write(k,v);
        }
    }
    public static class Step1Reducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer=new StringBuffer();
            for(Text text:values){
                stringBuffer.append(text.toString());
                stringBuffer.append(",");
            }
            context.write(key,new Text(stringBuffer.toString()));
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step1 <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(com.hadoop.usercf.Step1.class);
        job.setJobName("step1");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(com.hadoop.itemcf.Step1.Step1Mapper.class);
        job.setReducerClass(com.hadoop.itemcf.Step1.Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
