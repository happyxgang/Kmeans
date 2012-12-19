package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;

public class CustomArrayWritable  implements WritableComparable  {
	
@Override
	public String toString() {
		String str = "";
		for(Double d : this.list){
			str = str + d + " ";
		}
		
		return str;
	}

@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((list == null) ? 0 : list.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomArrayWritable other = (CustomArrayWritable) obj;
		if (list == null) {
			if (other.list != null)
				return false;
		} else if (!list.equals(other.list))
			return false;
		return true;
	}

	//	Map<K,Writable> m = new HashMap<K,Writable>();
	private ArrayList<Double> list = new ArrayList<Double>();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Long l = in.readLong();
		while (l > 0){
			l--;
			list.add(in.readDouble());
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong((long)list.size());
		for(Double d : list){
			out.writeDouble(d);
		}
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		CustomArrayWritable set = (CustomArrayWritable) o;
		if(!(this.list.size()== set.list.size())){
			return this.list.size() - set.list.size();
		} 
		
		return 0;
	}

	/**
	 * @param args
	 */
	public boolean add(Double d){
		//size++;
		return list.add(d);
	}
	public boolean add(Collection<Double> collection){
//		for(Double d : collection){
//			add(d);
//		}
//		return true;
		//size = (long) collection.size();
		list.clear();
		return list.addAll(collection);
	}
}
