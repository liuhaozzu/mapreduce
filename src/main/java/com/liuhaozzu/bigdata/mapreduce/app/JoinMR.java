package com.liuhaozzu.bigdata.mapreduce.app;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.liuhaozzu.bigdata.mapreduce.orderproduct.InfoBean;

public class JoinMR {
	static class JoinMRMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		InfoBean bean = new InfoBean();
		Text k = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String filename = inputSplit.getPath().getName();
			String pid = "";
			if (filename.startsWith("order")) {
				pid = fields[2];
				bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");
			} else {
				pid = fields[0];
				bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
			}
			k.set(pid);
			context.write(k, bean);
		}
	}

	static class JoinMRReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<InfoBean> beans,
				Reducer<Text, InfoBean, InfoBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			InfoBean pdBean = new InfoBean();
			ArrayList<InfoBean> orderBeans = new ArrayList<>();
			try {
				for (InfoBean bean : beans) {
					if ("1".equals(bean.getFlag())) {
						BeanUtils.copyProperties(pdBean, bean);
					} else {
						InfoBean odBean = new InfoBean();
						BeanUtils.copyProperties(odBean, bean);
						orderBeans.add(odBean);
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		}

	}
}
