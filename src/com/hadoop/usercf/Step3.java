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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/25
 *
 * @描述:第三步读取第一步的输出文件，执行一次双重循环：比如原始记录
 * 1\t272  2\t62  目标输出结果为1,2\t272*62。输出结果记录了这两个用户看过电影的乘机
 * 以便后续使用余弦相似度计算用户之间相似度
 */
public class Step3 {

    public static class Step3Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(1);
            Text v=new Text(tokens[0]+","+tokens[1]);
            context.write(k,v);
        }
    }

    public static class Step3Reducer extends Reducer<IntWritable,Text,Text,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer,Integer> map=new HashMap<>();
            for(Text text:values){
                String[]tokens=text.toString().split(",");
                map.put(Integer.valueOf(tokens[0]),Integer.valueOf(tokens[1]));
            }
            Iterator<Integer> iterator=map.keySet().iterator();
            while (iterator.hasNext()){
                Integer k1=iterator.next();
                Integer v1=map.get(k1);
                Iterator<Integer> iterator2=map.keySet().iterator();
                while (iterator2.hasNext()){
                    Integer k2=iterator2.next();
                    Integer v2=map.get(k2);
                    if(k1.equals(k2))
                        continue;
                    else {
                        Integer value=v1*v2;
                        Text k=new Text(k1+","+k2);
                        IntWritable v=new IntWritable(value);
                        context.write(k,v);
                    }
                }
            }
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step3 <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Step3.class);
        job.setJobName("step3");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step3Mapper.class);
        job.setReducerClass(Step3Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
