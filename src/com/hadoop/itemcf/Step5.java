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
import java.util.*;

/**
 * Created with IDEA
 * USER: DUAN
 * DATE: 2018/5/14.
 */
public class Step5 {
    public static class Step5Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []tokens=value.toString().split("\t");
            String []ids=tokens[0].split(",");
            int movieid=Integer.valueOf(ids[0]);
            String neighborid=ids[1];
            IntWritable k=new IntWritable(movieid);
            Text v=new Text(neighborid+","+tokens[1]);
            context.write(k,v);
        }
    }

    public static class Step5Reducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<ItemSimilarityModel,Integer> simMap=new HashMap<>();
            List<ItemSimilarityModel> simList=new ArrayList<>();
            for(Text text:values){
                ItemSimilarityModel model=new ItemSimilarityModel();
                model.setSourceItem(key.get());
                String tokens[]=text.toString().split(",");
                Double sim=Double.valueOf(tokens[1]);
                model.setSimilarity(sim);
                Integer targetId=Integer.valueOf(tokens[0]);
                model.setTargetItem(targetId);
                simMap.put(model,targetId);
                simList.add(model);
            }
            Collections.sort(simList,Collections.reverseOrder());
            //取每个item前10的相似item\
            StringBuilder stringBuilder=new StringBuilder();
            for(int i=0;i<10;i++){
                if(i>=simList.size())
                    break;
                else {
                    ItemSimilarityModel simmodel=simList.get(i);
                    Integer neighbor=simMap.get(simmodel);
                    stringBuilder.append(neighbor);
                    stringBuilder.append(",");
                    stringBuilder.append(String.format("%.5f",simmodel.getSimilarity()));
                    stringBuilder.append(":");
                }
            }
            Text v=new Text(stringBuilder.toString());
            context.write(key,v);
        }
    }
    public static void main(String []args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step5 <input path> <output path>");
            System.exit(-1);
        }
        Job job=Job.getInstance();
        job.setJarByClass(Step5.class);
        job.setJobName("step5");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step5.Step5Mapper.class);
        job.setReducerClass(Step5.Step5Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
