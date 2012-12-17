package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

public class WordTFIDFValues implements WritableComparable {
//	PriorityQueue<WordTFIDFValue> values = new PriorityQueue<WordTFIDFValue>(30 , new Comparator()// 大数在队列头
//            {
//        public int compare(Object o1, Object o2)
//        {
//        	WordTFIDFValue w1 = (WordTFIDFValue) o1;
//        	WordTFIDFValue w2 = (WordTFIDFValue) o2;
//        	
//            return w2.compareTo(w1);
//        }
//    });
	PriorityQueue<WordTFIDFValue> values = new PriorityQueue<WordTFIDFValue>(30);
	private Long size;
	private final Long MaxSize = 30L;
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordTFIDFValues other = (WordTFIDFValues) obj;
		if (size == null) {
			if (other.size != null)
				return false;
		} else if (!size.equals(other.size))
			return false;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
			return false;
		return true;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	
	@Override
	public String toString() {
		String str =  "[values=";
		for(WordTFIDFValue value : values){
			str += value;
		}
		str += "]";
		return str;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((size == null) ? 0 : size.hashCode());
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}



	public PriorityQueue<WordTFIDFValue> getValues() {
		return values;
	}

	public void setValues(PriorityQueue<WordTFIDFValue> values) {
		this.values = values;
	}

	public WordTFIDFValues(PriorityQueue<WordTFIDFValue> values) {
		super();
		this.values = values;
	}

	public WordTFIDFValues() {
		size = 0L;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		WordTFIDFValues values = (WordTFIDFValues) o;

		return this.size.compareTo(values.getSize());

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		size = in.readLong();
		System.out.println("WordTFIDFValues ReadField: " + size);
		long l = size.longValue();
		while (l > 0) {
			l--;
			WordTFIDFValue wTFIDFTmp = new WordTFIDFValue();
			
//			wTFIDFTmp.setWord(in.readUTF());
		
//			wTFIDFTmp.setTfidf(in.readDouble());
			wTFIDFTmp.readFields(in);
			add(wTFIDFTmp);
			
			System.out.println(wTFIDFTmp.getWord());
			//System.out.println(wTFIDFTmp);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.size);
		System.out.println(this.size);
		int ll = 0;
		for (WordTFIDFValue value : values) {
			System.out.println(value.getWord());
			value.write(out);
			ll++;
		}
		System.out.println("WordTFIDFValues Write" + size);

	}

	public void add(WordTFIDFValue value) {
		if(!this.values.isEmpty()){
			WordTFIDFValue w = this.values.peek();
			if(this.size < this.MaxSize){
				size++;
				this.values.add(value);	
			}else{
				if(value.tfidf > w.getTfidf()){
					values.poll();
					values.add(value);
				}				
			}
		}else{
			size++;
			this.values.add(value);
		}
	}

}
