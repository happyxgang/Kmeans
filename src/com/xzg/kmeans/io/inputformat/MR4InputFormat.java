package com.xzg.kmeans.io.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.recordreader.MR3RecordReader;
import com.xzg.kmeans.io.recordreader.MR4RecordReader;

public class MR4InputFormat extends
FileInputFormat<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue>{

	@Override
	public RecordReader<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO A	uto-generated method stub
				
		return new MR4RecordReader();
	}

}
