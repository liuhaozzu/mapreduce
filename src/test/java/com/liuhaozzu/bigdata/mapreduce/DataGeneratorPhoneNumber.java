package com.liuhaozzu.bigdata.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class DataGeneratorPhoneNumber {
	public static void main(String[] args) throws FileNotFoundException {
		File fOut = new File("test.text");
		FileChannel fcout = new RandomAccessFile(fOut, "rws").getChannel();
		int size = "16618776302 555 8999\n".getBytes().length;
		ByteBuffer wBuffer = ByteBuffer.allocateDirect(size);
		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator();
		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			String line = phoneNumberGenerator.generate() + " " + random.nextInt(1000) + " " + random.nextInt(10000)
					+ "\n";
			writeLine(fcout, wBuffer, line);
		}
	}

	public static void writeLine(FileChannel fcout, ByteBuffer wBuffer, String line) {
		try {
			fcout.write(wBuffer.wrap(line.getBytes()), fcout.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
