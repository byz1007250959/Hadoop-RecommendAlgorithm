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
 * DATE: 2018/4/25
 *
 * @描述:第四步读取第二步的输出文件，通过reduce排序合并的功能寻找出用户与用户之间电影的
 * 交集个数，例如原始文件：1,2\t1 输出文件1,2\t34 表示用户1和用户2看过电影的交集个数为34个
 */
public class Step4 {
    public static class Step4Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            Text k=new Text(tokens[0]);
            IntWritable v=new IntWritable(1);
            context.write(k,v);
        }
    }

    public static class Step4Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int mixed=0;
            for(IntWritable intWritable:values){
                mixed++;
            }
            IntWritable v=new IntWritable(mixed);
            context.write(key,v);
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step4 <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Step4.class);
        job.setJobName("step4");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step4Mapper.class);
        job.setReducerClass(Step4Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
