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
import java.util.*;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/27
 *
 * @描述:第六步读取第五步的输出文件，为每一个用户计算出与他最为相似的10位邻居
 * 并输出到文件，输出结果为:
 * 1\t916,0.52786:92,0.52022:268,0.51224:864,0.50922:301,0.50823:435,0.50456:823,0.50374:738,0.49841:293,0.49559:457,0.49182:
 * 这一行记录了与用户1最相似的10个邻居及他们之间的相似度。
 */
public class Step6 {

    public static class Step6Mapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []tokens=value.toString().split("\t");
            int userid=Integer.valueOf(tokens[0]);
            String neighborMsg=tokens[1];
            IntWritable k=new IntWritable(userid);
            Text v=new Text(neighborMsg);
            context.write(k,v);
        }
    }

    public static class Step6Reducer extends Reducer<IntWritable,Text,IntWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Double,Integer> simMap=new HashMap<>();
            List<Double> simList=new ArrayList<>();
            for(Text text:values){
                String tokens[]=text.toString().split(",");
                Double sim=Double.valueOf(tokens[1]);
                Integer neighborId=Integer.valueOf(tokens[0]);
                simMap.put(sim,neighborId);
                simList.add(sim);
            }
            Collections.sort(simList,Collections.reverseOrder());
            //取每个用户前10的邻居\
            StringBuilder stringBuilder=new StringBuilder();
            for(int i=0;i<10;i++){
                if(i>=simList.size())
                    break;
                else {
                    double sim=simList.get(i);
                    Integer neighbor=simMap.get(sim);
                    stringBuilder.append(neighbor);
                    stringBuilder.append(",");
                    stringBuilder.append(String.format("%.5f",sim));
                    stringBuilder.append(":");
                }
            }
            Text v=new Text(stringBuilder.toString());
            context.write(key,v);
        }
    }

    public static void main(String []args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step6 <input path> <output path>");
            System.exit(-1);
        }
        Job job=Job.getInstance();
        job.setJarByClass(Step6.class);
        job.setJobName("step6");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step6Mapper.class);
        job.setReducerClass(Step6Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
