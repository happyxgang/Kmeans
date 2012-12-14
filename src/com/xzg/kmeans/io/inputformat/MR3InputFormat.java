package com.xzg.kmeans.io.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;

public class MR3InputFormat extends
FileInputFormat<WordDocumentKey, WordNumDocumentWordNumValue>{

	@Override
	public RecordReader<WordDocumentKey, WordNumDocumentWordNumValue> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO A	uto-generated method stub
		// fuck you babk
		System.out.println();
		return null;
	}

}
