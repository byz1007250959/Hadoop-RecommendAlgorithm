package com.hadoop.usercf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/5/2
 *
 * @描述:读取第六步的输出文件和原始文件，将电影信息与用户的邻居信息关联起来
 * 同时构造电影的倒排信息，在map阶段以电影id为key，在reduce阶段以每一部电影对所有
 * 的用户计算一次用户对该电影的感兴趣程度。输出结果形如：
 * 1	1,20.29633
 * 2	1,10.23537
 * 表示用户1对电影id为1的电影感兴趣程度为20,29633
 * 用户2对电影id为1的电影感兴趣程度为10.23537
 */
public class Step7 {
    public static class Step7Mapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        private String inputsource;//a表示step6的输出文件,b表示原始文件
        private static Integer maxItemId=1682;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split=(FileSplit) context.getInputSplit();
            inputsource=split.getPath().getParent().getName();
            System.out.println(inputsource);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(inputsource.equals("step6")){
                for(int i=1;i<=maxItemId;i++){
                    IntWritable k=new IntWritable(i);
                    Text v=new Text("A"+value);
                    context.write(k,v);
                }
            }
            else if(inputsource.equals("data")){
                String tokens[]=value.toString().split("\t");
                IntWritable k=new IntWritable(Integer.valueOf(tokens[1]));
                Text v=new Text("B"+tokens[0]+","+tokens[2]);
                context.write(k,v);
            }
        }
    }

    public static class Step7Reducer extends Reducer<IntWritable,Text,IntWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer,Integer> currentMovieRatingMap=new HashMap<>();
            Map<Integer,String>  userSimilarityMap=new HashMap<>();
            /*
            遍历一次values，构造当前key这部电影的相关信息，准备对所有用户计算一次对该
            影片的感兴趣程度
             */
            for(Text text:values){
                String val=text.toString();
                if(val.startsWith("A")){
                    //数据来源于step6的输出
                    String step6=val.substring(1);
                    String step6tokens[]=step6.split("\t");
                    Integer userid=Integer.valueOf(step6tokens[0]);
                    userSimilarityMap.put(userid,step6tokens[1]);
                }
                else {
                    //数据来源于原始文件
                    String source=val.substring(1);
                    String sourceToken[]=source.split(",");
                    Integer userid=Integer.valueOf(sourceToken[0]);
                    Integer rating=Integer.valueOf(sourceToken[1]);
                    currentMovieRatingMap.put(userid,rating);
                }
            }
            //下面进行推荐度的计算
            Iterator<Integer> iterator=userSimilarityMap.keySet().iterator();
            //对每一个用户计算该影片的推荐度
            while (iterator.hasNext()){
                Integer userid=iterator.next();
                String simStr=userSimilarityMap.get(userid);
                String neighborsInfo[]=simStr.split(":");
                double sumsim=0;
                for(String neighbor:neighborsInfo){
                    if(neighbor.equals(""))
                        continue;
                    String neighborToken[]=neighbor.split(",");
                    Integer neighborId=Integer.valueOf(neighborToken[0]);
                    Double sim=Double.valueOf(neighborToken[1]);
                    if(currentMovieRatingMap.containsKey(neighborId)){
                        int score=currentMovieRatingMap.get(neighborId);
                        sumsim+=sim*score;
                    }
                }
                IntWritable k=new IntWritable(userid);
                Text v=new Text(key.toString()+","+sumsim);
                context.write(k,v);
            }
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 3) {
            System.err.println("Usage: Step7 <input path> <output path>");
            System.exit(-1);
        }
        Job job=Job.getInstance();
        job.setJarByClass(Step7.class);
        job.setJobName("Step7");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Step7Mapper.class);
        job.setReducerClass(Step7Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
