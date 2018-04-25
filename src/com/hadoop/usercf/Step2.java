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
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/19
 *
 * @描述:第二步将原始数据分解成电影倒排表信息，对看过某一部电影的用户进行两两计算，表示
 * 他们之间的存在一部交集电影。文件输出形式为：892,622\t1 表示用户892和622之间有一部交集的电影
 * 这个文件用于后续计算两两用户之间所有的相交电影个数
 */
public class Step2 {
    public static class Step2Mapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[1]));
            IntWritable v=new IntWritable(Integer.valueOf(tokens[0]));
            context.write(k,v);
        }
    }
    public static class Step2Recuder extends Reducer<IntWritable,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> list=new ArrayList<>();
            for(IntWritable value:values){
                Integer v=value.get();
                list.add(v);
            }
            for(int i=0;i<list.size();i++){
                for(int j=0;j<list.size();j++){
                        Integer value=list.get(i);
                        Integer value2=list.get(j);
                        if(value.equals(value2))
                            continue;
                        Text k=new Text(value+","+value2);
                        IntWritable v=new IntWritable(1);
                        context.write(k,v);
                    }
                }
            }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step2 <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Step2.class);
        job.setJobName("step2");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step2Mapper.class);
        job.setReducerClass(Step2Recuder.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
