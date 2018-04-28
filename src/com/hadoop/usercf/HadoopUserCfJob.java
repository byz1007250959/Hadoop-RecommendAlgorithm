package com.hadoop.usercf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/27
 *
 * @描述:这个类将所有的任务连接起来成为一个整体
 */
public class HadoopUserCfJob {
    public static void main(String args[])throws Exception{
        long start=System.currentTimeMillis();
        String sourcePath="D:/input/data";
        String step1out="D:/output/step1";
        String step3out="D:/output/step3";
        String step2out="D:/output/step2";
        String step4out="D:/output/step4";
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
       // step1.waitForCompletion(true);

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
        //step3.waitForCompletion(true);

        //step2设置
        Job step2=Job.getInstance();
        step2.setJarByClass(Step2.class);
        step2.setJobName("step2");
        step2.setMapperClass(Step2.Step2Mapper.class);
        step2.setReducerClass(Step2.Step2Recuder.class);
        step2.setMapOutputKeyClass(IntWritable.class);
        step2.setMapOutputValueClass(IntWritable.class);
        step2.setOutputKeyClass(Text.class);
        step2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(step2, new Path(sourcePath));
        FileOutputFormat.setOutputPath(step2, new Path(step2out));

        //step4设置
        Job step4=Job.getInstance();
        step4.setJarByClass(Step4.class);
        step4.setJobName("step4");
        step4.setMapperClass(Step4.Step4Mapper.class);
        step4.setReducerClass(Step4.Step4Reducer.class);
        step4.setMapOutputKeyClass(Text.class);
        step4.setMapOutputValueClass(IntWritable.class);
        step4.setOutputKeyClass(Text.class);
        step4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(step4, new Path(step2out));
        FileOutputFormat.setOutputPath(step4, new Path(step4out));

        ControlledJob controlledJob1=new ControlledJob(step1.getConfiguration());
        ControlledJob controlledJob2=new ControlledJob(step2.getConfiguration());
        ControlledJob controlledJob3=new ControlledJob(step3.getConfiguration());
        ControlledJob controlledJob4=new ControlledJob(step4.getConfiguration());
        controlledJob3.addDependingJob(controlledJob1);
        controlledJob4.addDependingJob(controlledJob2);
        JobControl jobControl=new JobControl("usercfControl");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);
        Thread thread=new Thread(jobControl);
        thread.start();
        while (!jobControl.allFinished()){
            Thread.sleep(1000);
        }
        long end=System.currentTimeMillis();
        System.out.println("计算共花费时间:"+(end-start)/1000+"秒");
        System.exit(0);
    }
}
