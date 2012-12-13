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

public class MR2RecordReader extends
		RecordReader<WordDocumentKey, IntWritable> {

	private LineRecordReader lineReader;
	private WordDocumentKey wdkey;
	private IntWritable value;

	public MR2RecordReader() {
		lineReader = new LineRecordReader();
		wdkey = new WordDocumentKey();
		value = new IntWritable();
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
			//System.out.println(currentValue);
			String[] result = currentValue.split("\\s");
			if (result.length == 2) {
				value.set(Integer.valueOf(result[1].trim()));
				String[] wdStr = result[0].split(",");
				if (wdStr.length == 2) {
					wdkey.setWord(wdStr[0]);
					wdkey.setDocumentNum(Long.valueOf(wdStr[1].trim()));
				} else {
					return false;
				}
				//System.out.println("value : " + value);
				//System.out.println("wdKey : " + wdkey);
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
	public IntWritable getCurrentValue() throws IOException,
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