package com.xzg.kmeans.io.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.xzg.kmeans.io.recordreader.MR2RecordReader;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;

public class MR2InputFormat extends
    FileInputFormat<WordDocumentKey, IntWritable> {

	@Override
	public RecordReader<WordDocumentKey, IntWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new MR2RecordReader();
	}


}