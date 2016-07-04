package GetEdge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 16-5-29.
 */
public class GetEdge {

    /**
     * Mapper类实现，用于获取每条边
     */
    public static class GetEdgeMapper extends Mapper<Object,Text,Text,Text>{

        Text outkey = new Text();
        Text outValue = new Text("");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []pointId = value.toString().split(" ");
//            int Id1 = Integer.parseInt(pointId[0]);
//            int Id2 = Integer.parseInt(pointId[1]);
            String edge=null;
            if(pointId[0].compareTo(pointId[1])<0){
                edge=pointId[0]+" "+pointId[1];
                outkey.set(edge);
                context.write(outkey,outValue);
            }else if(pointId[0].compareTo(pointId[1])>0){
                edge=pointId[1]+" "+pointId[0];
                outkey.set(edge);
                context.write(outkey,outValue);
            }
        }
    }


    /**
     * 只写一遍，用于删除重复的边
     */
    public static class GetEdgeReducer extends Reducer<Text,Text,Text,Text>{
        Text outvalue = new Text("");
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,outvalue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "Get Edge");
        job1.setJarByClass(GetEdge.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(GetEdgeMapper.class);
        job1.setReducerClass(GetEdgeReducer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
    }
}
