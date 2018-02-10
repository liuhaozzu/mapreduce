package com.liuhaozzu.bigdata.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		return (key.getItemId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
