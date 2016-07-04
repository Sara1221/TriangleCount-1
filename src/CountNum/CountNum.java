package CountNum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hadoop on 16-5-29.
 */
public class CountNum {

    /**
     * Mapper类
     * 提取出每条需要检测的边和已知的边
     */
    public static class CountNumMapper extends Mapper<Object,Text,Text,Text>{
        Text outkey = new Text();
        Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            outkey.set(line[0]);
            outValue.set("tag:"+line[1]);//设置一个标记用于区分
            context.write(outkey,outValue);
            //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!"+line[1]);
            if (line[1].contains(",")){
                String[] str=line[1].split(",");
                //System.out.println(str.length);
                for (int i=0;i<str.length-1;i++){
                    for (int j=i+1;j<str.length;j++){
                        //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!"+i+"\t"+j+"\t"+str[i]+"\t"+str[j]);
                        outkey.set(str[i]);
                        outValue.set(str[j]);
                        context.write(outkey,outValue);
                    }
                }
            }
        }
    }

    public static class CountNumReducer extends Reducer<Text,Text,IntWritable,Text>{

        int num;

        Text outValue = new Text("");
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String,Boolean> map = new HashMap<String,Boolean>();
            ArrayList<String>list = new ArrayList<String>();
            for (Text val:values){
                String tmp = val.toString();
                if (tmp.startsWith("tag:")){
                    tmp=tmp.substring(4);
                    String[] array = tmp.split(",");
                    for (int i=0;i<array.length;i++){
                        map.put(array[i],true);

                    }
                } else{
                    list.add(tmp);
                }
            }
            for (int i=0;i<list.size();i++){
                if (map.containsKey(list.get(i))){
                    num++;
                }
            }
        }

        public void cleanup( Reducer<Text, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            IntWritable key = new IntWritable();
            key.set(num);
            context.write(key,outValue);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job2 = new Job(conf, "Count Numbers");
        //job2.setNumReduceTasks(1);
        job2.setJarByClass(CountNum.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(CountNumMapper.class);
        job2.setReducerClass(CountNumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);

    }
}
