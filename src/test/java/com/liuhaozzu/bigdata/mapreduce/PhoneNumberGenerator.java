package com.liuhaozzu.bigdata.mapreduce;

import java.util.Map;
import java.util.Random;

import org.jboss.netty.util.internal.ConcurrentHashMap;

public class PhoneNumberGenerator {

	private static final Map<String, String> container = new ConcurrentHashMap<>();
	private static final Random RANDOM = new Random();
	private static final String[] pool = { "13265509538", "13825661922", "15926916088", "15915036280", "13732360162",
			"13820979811" };

	public static String generate() {

		return pool[RANDOM.nextInt(pool.length)];
	}

}
