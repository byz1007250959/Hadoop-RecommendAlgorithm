package com.hadoop.itemcf;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IDEA
 * USER: DUAN
 * DATE: 2018/5/14.
 */
public class Step4 {
    public static class Step4Mapper extends Mapper<LongWritable,Text,Text,Text> {
        private String inputsource; //第二步输出用a表示第三步输出用b表示

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split=(FileSplit) context.getInputSplit();
            inputsource=split.getPath().getParent().getName();
            System.out.println(inputsource);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(inputsource.equals("step2")){
                String tokens[]=value.toString().split("\t");
                Text k=new Text(tokens[0]);
                Text v=new Text("A"+tokens[1]);
                context.write(k,v);
            }
            else if(inputsource.equals("step3")){
                String tokens[]=value.toString().split("\t");
                Text k=new Text(tokens[0]);
                Text v=new Text("B"+tokens[1]);
                context.write(k,v);
            }
        }
    }

    public static class Step4Reducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int mul=0;
            int mix=0;
            Iterator<Text> iterator=values.iterator();
            while (iterator.hasNext()){
                String value=iterator.next().toString();
                if(value.startsWith("A")){
                    String step2=value.substring(1);
                    mix=Integer.valueOf(step2);
                }
                else if(value.startsWith("B")){
                    String step3=value.substring(1);
                    mul=Integer.valueOf(step3);
                }
            }
            if(mul!=0&&mix!=0){
                double sim=mix/Math.sqrt(mul*1.0);
                Text v=new Text(String.format("%.5f",sim));
                context.write(key,v);
            }
        }
    }

    public static void main(String args[]) throws Exception{
        if (args.length != 3) {
            System.err.println("Usage: Step4 <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(Step4.class);
        job.setJobName("step4");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Step4Mapper.class);
        job.setReducerClass(Step4Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
