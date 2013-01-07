package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomTextWritable extends Text {
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return index.toString() + super.toString();
	}

	IntWritable index;
	
	public CustomTextWritable() {
		super();
		// TODO Auto-generated constructor stub
		index = new IntWritable(10);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		super.readFields(in);
		index.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		super.write(out);
		index.write(out);
	}

	
	
	
}
