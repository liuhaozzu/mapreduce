package com.liuhaozzu.bigdata.mapreduce.flow.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {
	private long upFlow;
	private long dFlow;
	private long sumFlow;

	public FlowBean() {
		// TODO Auto-generated constructor stub
	}

	public FlowBean(long upFlow, long dFlow) {
		super();
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = this.upFlow + this.dFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		dFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return "FlowBean [upFlow=" + upFlow + ", dFlow=" + dFlow + ", sumFlow=" + sumFlow + "]";
	}

}
