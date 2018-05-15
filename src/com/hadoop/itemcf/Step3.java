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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IDEA
 * USER: DUAN
 * DATE: 2018/5/14.
 */
public class Step3 {
    private static Map<Integer,Integer> map=new HashMap<>();
    public static class Step3Mapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[1]));
            IntWritable v=new IntWritable(Integer.valueOf(tokens[0]));
            context.write(k,v);
        }
    }
    public static class Step3Reducer extends Reducer<IntWritable,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable value:values){
                count++;
            }
            map.put(key.get(),count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Integer> movieIds=map.keySet();
            for(Integer id1:movieIds){
                for(Integer id2:movieIds){
                    if(id1.equals(id2))
                        continue;
                    int count1=map.get(id1);
                    int count2=map.get(id2);
                    Text k=new Text(id1+","+id2);
                    context.write(k,new IntWritable(count1*count2));
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
        job.setJarByClass(com.hadoop.itemcf.Step3.class);
        job.setJobName("step3");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(com.hadoop.itemcf.Step3.Step3Mapper.class);
        job.setReducerClass(com.hadoop.itemcf.Step3.Step3Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
