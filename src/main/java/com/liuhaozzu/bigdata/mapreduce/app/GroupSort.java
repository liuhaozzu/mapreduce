package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.liuhaozzu.bigdata.mapreduce.order.ItemIdPartitioner;
import com.liuhaozzu.bigdata.mapreduce.order.MyGroupingComparator;
import com.liuhaozzu.bigdata.mapreduce.order.OrderBean;

public class GroupSort {
	private static final Logger LOGGER = LogManager.getLogger(GroupSort.class);

	static class SortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			OrderBean bean = new OrderBean();
			String line = value.toString();
			String[] fields = line.split(",");
			bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));
			context.write(bean, NullWritable.get());
		}
	}

	static class SortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,
				Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// create task
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GroupSort.class);

		// 任务输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定Map Reduce
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setPartitionerClass(ItemIdPartitioner.class);

		job.setNumReduceTasks(2);

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
