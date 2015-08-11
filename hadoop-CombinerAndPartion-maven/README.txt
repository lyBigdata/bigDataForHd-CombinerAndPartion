项目介绍
        本项目我们使用明星搜索指数数据，分别统计出搜索指数最高的男明星和女明星。
数据集
		明星搜索指数数据集,数据格式:明星名字   \t  性别   \t   搜索次数 
思路分析
        基于项目的需求，我们通过以下几步完成：
1、编写 Mapper类，按需求将数据集解析为 key=gender，value=name+hotIndex，然后输出。
2、编写 Combiner 类，在map端合并 Mapper 输出结果，减少map写入磁盘的数据量和reduce时shuffle网络传输的数据量，
		然后输出给 Reducer。
		public static class ActorCombiner extends Reducer<Text, Text, Text, Text>
		《combiner可看做本地的reduce》
3、编写 Partitioner 类，按性别，将结果指定给不同的 Reduce 执行。
4、编写 Reducer 类，分别统计出男、女明星的最高搜索指数。
5、编写 run 方法执行 MapReduce 任务。