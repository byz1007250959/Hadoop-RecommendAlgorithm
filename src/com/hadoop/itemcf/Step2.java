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
public class Step2 {
    private static  int[][] itemsMixed=new int[1682][1682];
    public static class Step2Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[0]));
            Text v=new Text(tokens[1]);
            context.write(k,v);
        }
    }
    public static class Step2Reducer extends Reducer<IntWritable,Text,Text,IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           for(Text value:values){
               String line=value.toString();
               String ids[]=line.split(",");
               for(String id1:ids){
                   for(String id2:ids){
                       if(id1.equals(id2))
                           continue;
                       int i1=Integer.valueOf(id1);
                       int i2=Integer.valueOf(id2);
                       itemsMixed[i1-1][i2-1]+=1;
                   }
               }
           }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<1682;i++){
                for(int j=0;j<1682;j++){
                    int mixed=itemsMixed[i][j];
                    if(mixed==0)
                        continue;
                    Integer movieid1=i+1;
                    Integer movieid2=j+1;
                    Text k=new Text(movieid1+","+movieid2);
                    IntWritable v=new IntWritable(mixed);
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
        job.setJarByClass(com.hadoop.itemcf.Step2.class);
        job.setJobName("step2");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(com.hadoop.itemcf.Step2.Step2Mapper.class);
        job.setReducerClass(com.hadoop.itemcf.Step2.Step2Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
