package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

public class CustomHashMapWritable  implements WritableComparable  {
	
@Override
	public String toString() {
		String str = "";
		Set<Entry<String, Double>> set = hmap.entrySet();
		for(Entry<String, Double> entry : set){
			String key = entry.getKey();
			Double d = entry.getValue();
			str = str + key + ":" + d +" ";
		}
		return str;
	}

@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hmap == null) ? 0 : hmap.hashCode());
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
		CustomHashMapWritable other = (CustomHashMapWritable) obj;
		if (hmap == null) {
			if (other.hmap != null)
				return false;
		} else if (!hmap.equals(other.hmap))
			return false;
		return true;
	}

	//	Map<K,Writable> m = new HashMap<K,Writable>();
	private HashMap<String, Double> hmap = new HashMap<String, Double>();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Undefined!");

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Undefined!");
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		CustomHashMapWritable set = (CustomHashMapWritable) o;
		if(!(this.hmap.size()== set.hmap.size())){
			return this.hmap.size() - set.hmap.size();
		} 
		
		return 0;
	}


	public boolean add(HashMap<String, Double> hm){

		hmap = hm;
		return true;
	}
}
