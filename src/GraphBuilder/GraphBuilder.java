package GraphBuilder;

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
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by hadoop on 16-5-29.
 */
public class GraphBuilder {

    /**
     * Mapper类，将边的两点分开，小的值作为key，大的值作为value
     */
    public static class GraphBuilderMapper extends Mapper<Object,Text,Text,Text> {

        Text outKey = new Text();
        Text outValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //获取两个点的序号，注意每行最后有一个"\t"
            String temp = value.toString().split("\t")[0];
            String pointId[]=temp.split(" ");
            outKey.set(pointId[0]);
            outValue.set(pointId[1]);
            context.write(outKey,outValue);
        }
    }

    public static class GraphBuilderReducer extends Reducer<Text,Text,Text,Text>{

        Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //使用treeSet排序
            TreeSet<String> treeSet = new TreeSet<String>();
            for (Text val:values){
                treeSet.add(val.toString());
            }

            String out = "";
            Iterator<String> it = treeSet.iterator();
            for(;it.hasNext();){
                out=out+it.next()+",";
            }
            //去掉最后的","
            out=out.substring(0,out.length()-1);
            outValue.set(out);
            context.write(key,outValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "Build Graph");
        job1.setJarByClass(GraphBuilder.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(GraphBuilderMapper.class);
        job1.setReducerClass(GraphBuilderReducer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
    }
}
