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
 * DATE: 2018/5/2
 *
 * @描述:读取第7步骤的输出文件，以用户的id为key，再reduce阶段对所有电影的
 * 感兴趣程度进行排序，对用户推荐排名前limitmovie部电影(目前暂定20)。输出结果形如：
 * 1	50,174,172,98,176,56,173,96,474,64,144,100,181,168,195,183,7,4,69,12,
 * 2	100,286,50,269,124,127,276,237,313,275,14,258,257,515,9,302,285,297,181,7,
 */
public class Step8 {
    public static class Step8Mapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[]=value.toString().split("\t");
            IntWritable k=new IntWritable(Integer.valueOf(tokens[0]));
            Text v=new Text(tokens[1]);
            context.write(k,v);
        }
    }

    public static class Step8Reducer extends Reducer<IntWritable,Text,IntWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<UserInterestLevel> sortList=new ArrayList<>();
            for(Text text:values){
                String tokens[]=text.toString().split(",");
                Integer movieId=Integer.valueOf(tokens[0]);
                Double interested=Double.valueOf(tokens[1]);
                if(interested.equals(0.0))
                    continue;
                UserInterestLevel userInterestLevel=new UserInterestLevel();
                userInterestLevel.setInterestLevel(interested);
                userInterestLevel.setMovieId(movieId);
                userInterestLevel.setUserId(Integer.valueOf(key.toString()));
                sortList.add(userInterestLevel);
            }
            StringBuilder recommendMovies=new StringBuilder();
            Collections.sort(sortList,Collections.reverseOrder());
            for(int i=0;i<20;i++){
                if(i==sortList.size())
                    break;
                UserInterestLevel level=sortList.get(i);
                Integer movieId=level.getMovieId();
                recommendMovies.append(movieId);
                recommendMovies.append(",");
            }
            Text v=new Text(recommendMovies.toString());
            context.write(key,v);
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: Step8 <input path> <output path>");
            System.exit(-1);
        }
        Job job=Job.getInstance();
        job.setJarByClass(Step8.class);
        job.setJobName("Step8");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Step8Mapper.class);
        job.setReducerClass(Step8Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
