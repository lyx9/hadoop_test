
public classWordCount1 {
public static final String INPUT_PATH ="hdfs://hadoop0:9000/in";
public static final String OUT_PATH ="hdfs://hadoop0:9000/out";
public static void main(String[] args)throws Exception
{Configuration conf = newConfiguration();
FileSystem fileSystem =FileSystem.get(conf);
if(fileSystem.exists(newPath(OUT_PATH))){}fileSystem.delete(newPath(OUT_PATH),true);
Job job = newJob(conf,WordCount1.class.getSimpleName());//1.0读取文件，解析成key,value对
FileInputFormat.setInputPaths(job,newPath(INPUT_PATH));//2.0写上自己的逻辑，对输入的可以，value进行处理，转换成新的key,value对进行输出
job.setMapperClass(MyMapper.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(LongWritable.class);//3.0对输出后的数据进行分区//4.0对分区后的数据进行排序，分组，相同key的value放到一个集合中//5.0对分组后的数据进行规约//6.0对通过网络将map输出的数据拷贝到reduce节点//7.0 写上自己的reduce函数逻辑，对map输出的数据进行处理
job.setReducerClass(MyReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(LongWritable.class);
FileOutputFormat.setOutputPath(job,new Path(OUT_PATH));
job.waitForCompletion(true);}
static class MyMapper extendsMapper<LongWritable, Text, Text, LongWritable>{
@Override
protected void map(LongWritablek1, Text v1,org.apache.hadoop.mapreduce.Mapper.Contextcontext)
throws IOException,InterruptedException
{String[] split =v1.toString().split("\t");
for(String words :split){context.write(split[3],1);}}}
static class MyReducer extends Reducer<Text,LongWritable, Text, LongWritable>{protected void reduce(Text k2,Iterable<LongWritable> v2,
org.apache.hadoop.mapreduce.Reducer.Contextcontext)
throws IOException,InterruptedException {
Long count = 0L;
for(LongWritable time :v2)
{
count += time.get();
}
context.write(v2, newLongWritable(count));}}}
