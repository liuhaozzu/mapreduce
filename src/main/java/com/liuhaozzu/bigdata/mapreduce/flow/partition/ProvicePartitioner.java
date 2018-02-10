package com.liuhaozzu.bigdata.mapreduce.flow.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.liuhaozzu.bigdata.mapreduce.flow.entity.FlowBean;

public class ProvicePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		return Integer.parseInt(prefix) % 2;
	}

}
