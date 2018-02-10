package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.liuhaozzu.bigdata.mapreduce.file.MyInputFormat;

public class Many2One {
	static class FileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey;

		@Override
		protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}

		@Override
		protected void map(NullWritable key, BytesWritable value,
				Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Many2One.class);

		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(FileMapper.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.out.println("word count task failed");
		} else {
			System.out.println("success");
		}
	}
}
