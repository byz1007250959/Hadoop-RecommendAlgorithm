package com.hadoop.usercf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
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
        String step5out="D:/output/step5";
        String step6out="D:/output/step6";
        String step7out="D:/output/step7";
        String step8out="D:/output/step8";
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

        //step5设置
        Job step5= Job.getInstance();
        step5.setJarByClass(Step5.class);
        step5.setJobName("step5");
        ChainMapper.addMapper(step5, Step5.Step5Mapper.class,LongWritable.class,Text.class,Text.class,Text.class,step5.getConfiguration());
        ChainReducer.setReducer(step5,Step5.Step5Reducer.class,Text.class,Text.class,Text.class,DoubleWritable.class,step5.getConfiguration());
        ChainReducer.addMapper(step5,Step5.Step5Mapper2.class,Text.class,DoubleWritable.class,IntWritable.class,Text.class,step5.getConfiguration());
        FileInputFormat.addInputPath(step5, new Path(step3out));
        FileInputFormat.addInputPath(step5,new Path(step4out));
        FileOutputFormat.setOutputPath(step5, new Path(step5out));

        //step6设置
        Job step6=Job.getInstance();
        step6.setJarByClass(Step6.class);
        step6.setJobName("step6");
        step6.setMapperClass(Step6.Step6Mapper.class);
        step6.setReducerClass(Step6.Step6Reducer.class);
        step6.setMapOutputKeyClass(IntWritable.class);
        step6.setMapOutputValueClass(Text.class);
        step6.setOutputKeyClass(IntWritable.class);
        step6.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(step6, new Path(step5out));
        FileOutputFormat.setOutputPath(step6, new Path(step6out));

        //step7设置
        Job step7=Job.getInstance();
        step7.setJarByClass(Step7.class);
        step7.setJobName("step7");
        step7.setMapperClass(Step7.Step7Mapper.class);
        step7.setReducerClass(Step7.Step7Reducer.class);
        step7.setMapOutputKeyClass(IntWritable.class);
        step7.setMapOutputValueClass(Text.class);
        step7.setOutputKeyClass(IntWritable.class);
        step7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(step7,new Path(step6out));
        FileInputFormat.addInputPath(step7,new Path(sourcePath));
        FileOutputFormat.setOutputPath(step7,new Path(step7out));

        //step8设置
        Job step8=Job.getInstance();
        step8.setJarByClass(Step8.class);
        step8.setJobName("step8");
        step8.setMapperClass(Step8.Step8Mapper.class);
        step8.setReducerClass(Step8.Step8Reducer.class);
        step8.setMapOutputKeyClass(IntWritable.class);
        step8.setMapOutputValueClass(Text.class);
        step8.setOutputKeyClass(IntWritable.class);
        step8.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(step8,new Path(step7out));
        FileOutputFormat.setOutputPath(step8,new Path(step8out));

        ControlledJob controlledJob1=new ControlledJob(step1.getConfiguration());
        ControlledJob controlledJob2=new ControlledJob(step2.getConfiguration());
        ControlledJob controlledJob3=new ControlledJob(step3.getConfiguration());
        ControlledJob controlledJob4=new ControlledJob(step4.getConfiguration());
        ControlledJob controlledJob5=new ControlledJob(step5.getConfiguration());
        ControlledJob controlledJob6=new ControlledJob(step6.getConfiguration());
        ControlledJob controlledJob7=new ControlledJob(step7.getConfiguration());
        ControlledJob controlledJob8=new ControlledJob(step8.getConfiguration());
        controlledJob3.addDependingJob(controlledJob1);
        controlledJob4.addDependingJob(controlledJob2);
        controlledJob5.addDependingJob(controlledJob3);
        controlledJob5.addDependingJob(controlledJob4);
        controlledJob6.addDependingJob(controlledJob5);
        controlledJob7.addDependingJob(controlledJob6);
        controlledJob8.addDependingJob(controlledJob7);
        JobControl jobControl=new JobControl("usercfControl");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);
        jobControl.addJob(controlledJob5);
        jobControl.addJob(controlledJob6);
        jobControl.addJob(controlledJob7);
        jobControl.addJob(controlledJob8);
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
