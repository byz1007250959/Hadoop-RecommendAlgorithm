package com.hadoop.usercf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/27
 *
 * @描述:
 */
public class HadoopUserCfJob {
    public static void main(String args[])throws Exception{
        String sourcePath="D:/input/data";
        String step1out="D:/output/step1";
        String step3out="D:/output/step3";
        //step1设置
        Job step1=Job.getInstance();
        step1.setJarByClass(Step1.class);
        step1.setJobName("step1");
        step1.setMapperClass(Step1.Step1Mapper.class);
        step1.setReducerClass(Step1.Step1Reducer.class);
        step1.setMapOutputKeyClass(IntWritable.class);
        step1.setMapOutputValueClass(Text.class);
        step1.setOutputKeyClass(IntWritable.class);
        step1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(step1, new Path(sourcePath));
        FileOutputFormat.setOutputPath(step1, new Path(step1out));
        step1.waitForCompletion(true);

        //step3设置
        Job step3=Job.getInstance();
        step3.setJarByClass(Step3.class);
        step3.setJobName("step3");
        step3.setMapperClass(Step3.Step3Mapper.class);
        step3.setReducerClass(Step3.Step3Reducer.class);
        step3.setMapOutputKeyClass(IntWritable.class);
        step3.setMapOutputValueClass(Text.class);
        step3.setOutputKeyClass(Text.class);
        step3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(step3, new Path(step1out));
        FileOutputFormat.setOutputPath(step3, new Path(step3out));
        step3.waitForCompletion(true);
    }
}
