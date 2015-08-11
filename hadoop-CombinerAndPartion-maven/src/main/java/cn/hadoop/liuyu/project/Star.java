package cn.hadoop.liuyu.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @function 统计分别统计出男女明星最大搜索指数
 */
public class Star extends Configured implements Tool {
	/**
	 * @function Mapper 解析明星数据
	 * @input key=偏移量  value=明星数据
	 * @output key=gender value=name+hotIndex
	 */
	public static class ActorMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//value=name+gender+hotIndex
			String[] tokens = value.toString().split("\t");//使用分隔符\t，将数据解析为数组 tokens
			String gender = tokens[1].trim();//性别
			String nameHotIndex = tokens[0] + "\t" + tokens[2];//名称和关注指数
			//输出key=gender value=name+hotIndex
			context.write(new Text(gender), new Text(nameHotIndex));
		}
	}

	/**
	 * @function Partitioner 根据sex选择分区
	 */
	public static class ActorPartitioner extends Partitioner<Text, Text> {		 
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) { 
        	
            String sex = key.toString();//按性别分区
            
            // 默认指定分区 0
            if(numReduceTasks==0)
            	return 0;
            
            //性别为male 选择分区0
            if(sex.equals("male"))             
                return 0;
            //性别为female 选择分区1
            if(sex.equals("female"))
            	return 1 % numReduceTasks;
            //其他性别 选择分区2
            else
                return 2 % numReduceTasks;
           
        }
    }

	/**
	 * @function 定义Combiner 合并 Mapper 输出结果
	 */
	public static class ActorCombiner extends Reducer<Text, Text, Text, Text> {
		private Text text = new Text();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			int maxHotIndex = Integer.MIN_VALUE;
			int hotIndex = 0;
			String name="";
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				if(hotIndex>maxHotIndex){
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
			}
			text.set(name+"\t"+maxHotIndex);
			context.write(key, text);
		}
	}
	/**
	 * @function Reducer 统计男、女明星最高搜索指数
	 * @input key=gender  value=name+hotIndex
	 * @output key=name value=gender+hotIndex(max)
	 */
	public static class ActorReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

			int maxHotIndex = Integer.MIN_VALUE;

			String name = " ";
			int hotIndex = 0;
			// 根据key，迭代 values 集合，求出最高搜索指数
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				if (hotIndex > maxHotIndex) {
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
			}
			context.write(new Text(name), new Text( key + "\t"+ maxHotIndex));
		}
	}

	/**
	 * @function 任务驱动方法
	 * @param args
	 * @return
	 * @throws Exception
	 */
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();//读取配置文件
		
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "star");//新建一个任务
		job.setJarByClass(Star.class);//主类
		
		job.setNumReduceTasks(2);//reduce的个数设置为2
		job.setPartitionerClass(ActorPartitioner.class);//设置Partitioner类
		
		
		
		job.setMapperClass(ActorMapper.class);//Mapper
		job.setMapOutputKeyClass(Text.class);//map 输出key类型
		job.setMapOutputValueClass(Text.class);//map 输出value类型
				
		job.setCombinerClass(ActorCombiner.class);//设置Combiner类
		
		job.setReducerClass(ActorReducer.class);//Reducer
		job.setOutputKeyClass(Text.class);//输出结果 key类型
		job.setOutputValueClass(Text.class);//输出结果 value类型
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		job.waitForCompletion(true);//提交任务
		return 0;
	}
	/**
	 * @function main 方法
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ec = ToolRunner.run(new Configuration(), new Star(), args);;
		System.exit(ec);
	}
}

