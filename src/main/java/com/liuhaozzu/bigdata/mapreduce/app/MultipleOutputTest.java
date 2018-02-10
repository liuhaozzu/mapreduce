package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MultipleOutputTest {
	private static final Logger LOGGER = LogManager.getLogger(MultipleOutputTest.class);

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			context.write(new Text(fields[0]), value);
		}
	}

	static class MyReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		@Override
		protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				multipleOutputs.write(NullWritable.get(), value, key.toString());
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MultipleOutputs.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);

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
