package com.xzg.kmeans.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;

public class M2DocumentWordNumMapper extends
		Mapper<WordDocumentKey, IntWritable, LongWritable, WordWordNumValue> {
	public static LongWritable docnum = new LongWritable(0);
	public static WordWordNumValue wwn = new WordWordNumValue();
	@Override
	protected void map(WordDocumentKey key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		docnum.set(key.getDocumentNum());
		wwn.setWord(key.getWord());
		wwn.setWordnum(value.get());
//		System.out.println(docnum);
//		System.out.println(wwn);
		context.write(docnum, wwn);
	}

}
