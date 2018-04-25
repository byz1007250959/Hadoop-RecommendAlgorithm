package com.hadoop.usercf;

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
 * USER: Administrator
 * DATE: 2018/4/19
 *
 * @描述:第一步操作原始文件，提取出一位用户看过的电影数目，文件输出结果：
 * 1\t272 表示用户1评价过的电影数为272部
 */
public class Step1 {

    public static class Step1Mapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[0]));
            Text v=new Text(tokens[1]+","+tokens[2]);
            context.write(k,v);
        }
    }
    public static class Step1Reducer extends Reducer<IntWritable,Text,IntWritable,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int length=0;
            for(Text text:values){
                length++;
            }
            context.write(key,new IntWritable(length));
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step1 <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Step1.class);
        job.setJobName("step1");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
