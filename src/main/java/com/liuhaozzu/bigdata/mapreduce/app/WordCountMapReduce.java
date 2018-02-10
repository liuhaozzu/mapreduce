package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.liuhaozzu.bigdata.mapreduce.mapper.WordCountMapper;
import com.liuhaozzu.bigdata.mapreduce.mapper.WordCountReducer;

public class WordCountMapReduce {
	private static final Logger LOGGER = LogManager.getLogger(WordCountMapReduce.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcound");
		job.setJarByClass(WordCountMapReduce.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		// 设置map 输出的key value 类型
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 设置reduce输出的key value 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		LOGGER.info("wait for commpletion...");
		boolean b = job.waitForCompletion(true);
		LOGGER.info("commpletion with result: " + b);
		if (!b) {
			System.out.println("word count task failed");
		} else {
			System.out.println("success");
		}
	}
}
