package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MapWritable<K,V>  implements WritableComparable  {
	
	Map<K,V> m = new HashMap<K,V>();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @param args
	 */


}
