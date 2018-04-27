package com.hadoop.usercf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/26
 *
 * @描述:第五步读取第三步和第四步的输出文件，通过余弦相似度公式将用户之间的相似度
 * 计算出来，再reduce结束后再衔接一个mapper将一行内容处理为如：5\t8,0.6一行的数据表示
 * 用户5与他的邻居8号用户的相似度为0.6
 */
public class Step5 {

    public static class Step5Mapper extends Mapper<LongWritable,Text,Text,Text>{
        private String inputsource; //第三步输出用a表示第四步输出用b表示

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split=(FileSplit) context.getInputSplit();
            inputsource=split.getPath().getParent().getName();
            System.out.println(inputsource);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           if(inputsource.equals("step3")){
                String tokens[]=value.toString().split("\t");
                Text k=new Text(tokens[0]);
                Text v=new Text("A"+tokens[1]);
                context.write(k,v);
           }
           else {
               String tokens[]=value.toString().split("\t");
               Text k=new Text(tokens[0]);
               Text v=new Text("B"+tokens[1]);
               context.write(k,v);
           }
        }
    }

    public static class Step5Reducer extends Reducer<Text,Text,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int mul=0;
            int mix=0;
            Iterator<Text> iterator=values.iterator();
            while (iterator.hasNext()){
                String value=iterator.next().toString();
                if(value.startsWith("A")){
                    String step3=value.substring(1);
                    mul=Integer.valueOf(step3);
                }
                else if(value.startsWith("B")){
                    String step4=value.substring(1);
                    mix=Integer.valueOf(step4);
                }
            }
            if(mul!=0&&mix!=0){
                double sim=mix/Math.sqrt(mul*1.0);
                DoubleWritable v=new DoubleWritable(sim);
                context.write(key,v);
            }
        }
    }

    public static class Step5Mapper2 extends Mapper<Text,DoubleWritable,IntWritable,Text>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String tokens[]=key.toString().split(",");
            Integer sourceUser=Integer.valueOf(tokens[0]);
            Integer neighbor=Integer.valueOf(tokens[1]);
            IntWritable k=new IntWritable(sourceUser);
            Text v=new Text(neighbor+","+value);
            context.write(k,v);
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 3) {
            System.err.println("Usage: Step5 <input path> <output path>");
            System.exit(-1);
        }
        Configuration configuration=new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Step5.class);
        job.setJobName("step5");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        ChainMapper.addMapper(job,Step5Mapper.class,LongWritable.class,Text.class,Text.class,Text.class,configuration);
        ChainReducer.setReducer(job,Step5Reducer.class,Text.class,Text.class,Text.class,DoubleWritable.class,configuration);
        ChainReducer.addMapper(job,Step5Mapper2.class,Text.class,DoubleWritable.class,IntWritable.class,Text.class,configuration);
//        job.setMapperClass(Step5Mapper.class);
//        job.setReducerClass(Step5Reducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
