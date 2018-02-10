package com.liuhaozzu.bigdata.mapreduce.file;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	private static final Logger LOGGER = LogManager.getLogger(MyInputFormat.class);

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		LOGGER.info(context.toString() + ">>> filename: " + filename);
		// 设置每个小文件不可分片，保证一个小文件生成一个key-value键值对
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		MyRecordReader recordReader = new MyRecordReader();
		// recordReader.ini
		return null;
	}

}
