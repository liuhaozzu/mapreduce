package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.liuhaozzu.bigdata.mapreduce.flow.entity.FlowBean;
import com.liuhaozzu.bigdata.mapreduce.flow.partition.ProvicePartitioner;

public class FlowCount {
	private static final Logger LOGGER = LogManager.getLogger(FlowCount.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCount.class);

		// 指定mapper reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		// 指定分区器
		job.setPartitionerClass(ProvicePartitioner.class);
		// 指定相应分区数量的reducetask
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

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

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(" ");
			String phoneNbr = fields[0];
			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
		}
	}

	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,
				Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
			long sum_upflow = 0;
			long sum_dflow = 0;
			for (FlowBean bean : values) {
				sum_upflow += bean.getUpFlow();
				sum_dflow += bean.getdFlow();
			}
			FlowBean result = new FlowBean(sum_upflow, sum_dflow);
			context.write(key, result);
		}
	}
}
