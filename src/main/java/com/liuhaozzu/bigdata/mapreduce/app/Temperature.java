package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Temperature {

	private static final Logger LOGGER = LogManager.getLogger(Temperature.class);

	static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			LOGGER.info("Before Mapper " + key + " ---> " + value);
			String line = value.toString();
			String year = line.substring(0, 4);
			int temperature = Integer.parseInt(line.substring(8));
			context.write(new Text(year), new IntWritable(temperature));
			LOGGER.info("After Mapper " + new Text(year) + " ---> " + new IntWritable(temperature));
		}
	}

	static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			StringBuilder sb = new StringBuilder();
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
				sb.append(value).append(", ");
			}
			LOGGER.info("Before Reduce: " + key + ", " + sb.toString());
			context.write(key, new IntWritable(maxValue));
			LOGGER.info("======" + "After Reduce: " + key + ", " + maxValue);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 输入路径
		// String dst = "hdfs://centos7-1:8020/test/temprature/intput";
		// 输出路径，必须是不存在的，空文件加也不行。
		// String dstOut = "hdfs://centos7-1:8020/test/temprature/output";

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", LocalFileSystem.class.getName());
		Job job = Job.getInstance(conf);

		// 如果要打成jar包运行，需要下面这句
		job.setJarByClass(Temperature.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		boolean result = job.waitForCompletion(true);
		LOGGER.info("result: " + result);
	}
}
