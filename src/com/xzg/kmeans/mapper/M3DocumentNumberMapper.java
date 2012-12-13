package com.xzg.kmeans.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xzg.kmeans.io.customtypes.DocumentWordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;

public class M3DocumentNumberMapper
		extends
		Mapper<WordDocumentKey, WordNumDocumentWordNumValue, Text, DocumentWordNumDocumentWordNumValue> {
	private static DocumentWordNumDocumentWordNumValue docwndwn = new DocumentWordNumDocumentWordNumValue();
	private static Text word = new Text();

	@Override
	protected void map(WordDocumentKey key, WordNumDocumentWordNumValue value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		docwndwn.setDocumentId(key.getDocumentNum());

		docwndwn.setDocumentWordNum(value.getDocumentWordNum());
		
		docwndwn.setWordNum(value.getWordNum());
		word.set(key.getWord());
		context.write(word, docwndwn);
	}
}
