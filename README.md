## TriangleCount 图的三角计数

以下图为例：

![有向图](http://7xszpw.com1.z0.glb.clouddn.com/10.png)

文件数据格式如下：

```
1 2
1 3
2 1
2 3
2 5
3 5
4 2
5 4
```
每一行表示一条有向边，数字表示顶点序号，中间以空格间隔。



### 1.将有向边转换为无向边
 
   第一个MapReduce用于从文件读取两两点之间的边并处理掉重复边。

#####   Map：从文件读取一行数据，即两点的序号，key选择其中较小的在前，value直接赋值""。

   ```
    /**
     * Mapper类实现，用于获取每条边
     */
    public static class GetEdgeMapper extends Mapper<LongWritable,Text,Text,Text>{

        Text outkey = new Text();
        Text outValue = new Text("");
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []pointId = value.toString().split(" ");//获取两个点的序号
            int Id1 = Integer.parseInt(pointId[0]);//将字符串转为Int
            int Id2 = Integer.parseInt(pointId[1]);
            String edge=null;
            if(Id1 < Id2){//取其中较小的在前作为key
                edge=Id1+" "+Id2;
                outkey.set(edge);
                context.write(outkey,outValue);
            }else if(Id1 > Id2){
                edge=Id2+" "+Id1;
                outkey.set(edge);
                context.write(outkey,outValue);
            }
        }
    }
   ```
#####   Reduce：将重复的边删除，如上图，`边1->2`和`边2->1`经过Map得到的`<key-value>`都是`<1 2,>`，出现重复，所以写入文件只写入一次。
   ```
    public static class GetEdgeReducer extends Reducer<Text,Text,Text,Text>{
        Text outvalue = new Text("");
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,outvalue);
        }
    }
   ```
   下图是第一次MapReduce得到的结果，其中`pagerank/output/temp1`是输出文件在当前HDFS文件系统的位置。
   
   ![中间文件一](http://7xszpw.com1.z0.glb.clouddn.com/2016-05-30%2001-38-16.png)
   
   我们可以看出，这一步其实是将有向图转换为无向图：

   ![无向图](http://7xszpw.com1.z0.glb.clouddn.com/10.1.png)

### 2. 生成邻接表

   第二个MapReduce用于生成类似邻接表的结构，便于下一步的计数。

#####   Map： 由于第一步的MapReduce已经使得点序号较小的值排在前面，这里Map操作只需要直接将key取第一个点的序号，value取第二个点的序号。这里需要注意些小问题，后面会详细说明。

   ```
    /**
     * Mapper类，将边的两点分开，小的值作为key，大的值作为value
     */
    public static class GraphBuilderMapper extends Mapper<LongWritable,Text,Text,Text> {

        Text outKey = new Text();
        Text outValue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取两个点的序号，注意每行最后有一个"\t"
            String temp = value.toString().split("\t")[0];
            String pointId[]=temp.split(" ");
            outKey.set(pointId[0]);
            outValue.set(pointId[1]);
            context.write(outKey,outValue);
        }
    }
   ```
#####   Reduce：Reduce操作只需要将Map得到的key相同对应的value按递增序合并到一起，这里用到了Java的TreeSet数据结构用于快速实现排序。
   ```
    public static class GraphBuilderReducer extends Reducer<Text,Text,Text,Text>{

        Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //使用treeSet排序
            TreeSet<Integer> treeSet = new TreeSet<Integer>();
            for (Text val:values){
                int des = Integer.parseInt(val.toString());
                treeSet.add(des);
            }

            String out = "";
            Iterator<Integer> it = treeSet.iterator();
            for(;it.hasNext();){
                out=out+it.next()+",";
            }
            //去掉最后的","
            out=out.substring(0,out.length()-1);
            outValue.set(out);
            context.write(key,outValue);
        }
    }
   ```
   下图是第二次MapReduce得到的结果：
   
   ![中间文件二](http://7xszpw.com1.z0.glb.clouddn.com/2016-05-30%2001-38-42.png)

### 3. 计数   
   
   由第二个MapReduce我们得到了每个顶点相邻的顶点集合，以第一行为例，我们现在知道`1   2,3`，说明1到2、1到3均有边，如果要构成三角形，只需要知道2到3之间有没有边就行了。而2和3之间是否有边可以从2的邻接表中得到。

#####   Map：key取第一个顶点序号，value分两种情况，一是顶点的整个邻接表，二是要判断到某顶点是否有边的顶点序号。

   以`2   3,4,5`为例：
   
   * key=2，value=tag:3,4,5
_//前面的"tag:"作为标志用于和下面的情况做区分_
   * key=3，value=4
_//后面顶点按许两两组合_
   * key=3，value=5
   * key=4，value=5

#####   这样Map的目的就是将key中已经有的边与要判断的边Map到一起。

   ```
    /**
     * Mapper类
     * 提取出每条需要检测的边和已知的边
     */
    public static class CountNumMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text outkey = new Text();
        Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
   ```

#####   Reduce：根据Map的结果计数

   以key=2为例，value中包括：

   * tag:3,4,5
   * 3 
_//由顶点1的Map得到_

也就是说，在Reduce中，我们知道了顶点2能到的点有3、4、5，而我们需要知道2到3是否有边存在，如果存在说明有一个三角形存在，计数加一。

   ```
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
   ```

   最终得到的三角形个数数据如下：
   
   ![最终结果](http://7xszpw.com1.z0.glb.clouddn.com/2016-05-30%2001-36-57.png)

### 问题与解决

第二个mapreduce报错

```
java.lang.Exception: java.lang.NumberFormatException: For input string: "26	"
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:529)
Caused by: java.lang.NumberFormatException: For input string: "26	"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Integer.parseInt(Integer.java:580)
	at java.lang.Integer.parseInt(Integer.java:615)
	at GraphBuilder.GraphBuilder$GraphBuilderReducer.reduce(GraphBuilder.java:48)
	at GraphBuilder.GraphBuilder$GraphBuilderReducer.reduce(GraphBuilder.java:41)
	at org.apache.hadoop.mapreduce.Reducer.run(Reducer.java:171)
	at org.apache.hadoop.mapred.ReduceTask.runNewReducer(ReduceTask.java:627)
	at org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:389)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$ReduceTaskRunnable.run(LocalJobRunner.java:319)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
16/05/29 22:31:06 INFO mapreduce.Job: Job job_local488634971_0002 running in uber mode : false
16/05/29 22:31:06 INFO mapreduce.Job:  map 100% reduce 0%
16/05/29 22:31:06 INFO mapreduce.Job: Job job_local488634971_0002 failed with state FAILED due to: NA
16/05/29 22:31:06 INFO mapreduce.Job: Counters: 35
```
原因：reduce写入文件时会在key和value之间自动加入`\t`，第一个value设置为`""`，但是最后还是有"\t"，由于这个的存在导致了String转Int时报错。


### 附加一

上面的实验中，从有向边到无向边的转换逻辑为

`IF(A->B)OR(B->A)THEN A-B`

现在将逻辑替换为

`IF(A->B)AND(B->A)THEN A-B`

我们可以看到，上述的边转换过程就是在第一个MapReduce中实现的，只要存在`1->2`或者`2->1`，都会被Map为<1 2,>的形式，而上面的Reduce是只要存在<key-value>就记录，这样就实现或的操作。要实现与操作，我们需要做稍微的修改，必须`A->B`和`B->A`同时存在，则Reduce得到的values的大小必须为2，所以我们在Reduce中加入判断即可：

```
    public static class GetEdgeReducer extends Reducer<Text,Text,Text,Text>{
        Text outvalue = new Text("");
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num=0;
            for (Text val:values){
                num++;
            }
            if(num==2){
                context.write(key,outvalue);
            }
        }
    }
```

### 附加二

Google+超大数据集，这里会面临一个问题，就是点太多，点的序号超出longwritable所能表示的范围，所以这里需要做些修改，把顶点的序号统一用字符串表示，大小比较也直接用字符串比较。

具体过程见代码，这里不详细介绍。
