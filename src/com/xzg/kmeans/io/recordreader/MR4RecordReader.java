package com.xzg.kmeans.io.recordreader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;

public class MR4RecordReader extends
		RecordReader<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> {

	private LineRecordReader lineReader;
	private WordDocumentKey wdkey;
	private WordNumDocumentWordNumDocumentNumValue value;

	public MR4RecordReader() {
		lineReader = new LineRecordReader();
		wdkey = new WordDocumentKey();
		value = new WordNumDocumentWordNumDocumentNumValue();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (lineReader.nextKeyValue()) {
			String currentValue = lineReader.getCurrentValue().toString();
			
			String[] result = currentValue.split("\\s");
			if (result.length == 2) {

				String[] wdStr = result[0].split(",");
				if (wdStr.length == 2) {
					wdkey.setWord(wdStr[0]);
					wdkey.setDocumentNum(Long.valueOf(wdStr[1].trim()));
				} else {
					return false;
				}
				String[] valueStr = result[1].split(",");
				if (valueStr.length == 3) {
					value.setWordNum(Long.valueOf(valueStr[0].trim()));
					value.setDocumentWordNum((Long.valueOf(valueStr[1].trim())));
					value.setDocumentNum(Long.valueOf(valueStr[2].trim()));
				} else {
					return false;
				}
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public WordDocumentKey getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.wdkey;
	}

	@Override
	public WordNumDocumentWordNumDocumentNumValue getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		return lineReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineReader.close();
	}
}