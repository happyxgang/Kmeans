package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;

public class VocabularyValue implements WritableComparable {
	public HashSet<String> getWords() {
		return words;
	}

	public void setWords(HashSet<String> words) {
		this.words = words;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	HashSet<String> words = new HashSet<String>();
	Long size;
	@Override
	public String toString() {
		String str = "";
		Iterator itr = words.iterator();
		while(itr.hasNext()){
			String s = (String) itr.next();
			str = str+"$$" + s;
		}
		
		return str;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((size == null) ? 0 : size.hashCode());
		result = prime * result + ((words == null) ? 0 : words.hashCode());
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
		VocabularyValue other = (VocabularyValue) obj;
		if (size == null) {
			if (other.size != null)
				return false;
		} else if (!size.equals(other.size))
			return false;
		if (words == null) {
			if (other.words != null)
				return false;
		} else if (!words.equals(other.words))
			return false;
		return true;
	}



	public VocabularyValue() {
		size = 0L;
	}

	public boolean add(String str) {
		if (words.add(str)) {
			size++;
			return true;
		} else {

			return false;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		size = in.readLong();
		//System.out.println("VocabularyValue" + size);
		Long l = size.longValue();
		//Long ll = 0L;
		while(l > 0){
			String str = new String(in.readUTF());
			add(str);
			l--;
			//ll++;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(size);
		for(String str : words){
			out.writeUTF(str);
		}
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		VocabularyValue v = (VocabularyValue)o;
	
		return this.size.compareTo(v.size);
	}

}
