package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordTFIDFValue implements WritableComparable {

	String word;
	Double tfidf;

	@Override
	public String toString() {
		return "[word=" + word + ", tfidf=" + tfidf + "]";
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Double getTfidf() {
		return tfidf;
	}

	public void setTfidf(Double tfidf) {
		this.tfidf = tfidf;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tfidf == null) ? 0 : tfidf.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WordTFIDFValue other = (WordTFIDFValue) obj;
		if (tfidf == null) {
			if (other.tfidf != null)
				return false;
		} else if (!tfidf.equals(other.tfidf))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	public WordTFIDFValue() {
		this.word = new String();
		this.tfidf = (double) 0;
	}

	public WordTFIDFValue(String word, Double tfidf) {
		super();
		this.word = word;
		this.tfidf = tfidf;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word = in.readUTF();
		tfidf = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(word);
		out.writeDouble(tfidf);

	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		WordTFIDFValue wTFIDF = (WordTFIDFValue) o;
//		if (this.word.equals(wTFIDF.getWord())) {
//			return this.tfidf.compareTo(wTFIDF.getTfidf());
//		} else {
//			return this.word.compareTo(wTFIDF.getWord());
//		}
		if(this.tfidf.equals(wTFIDF.getTfidf())){
			return this.word.compareTo(wTFIDF.getWord());
		}else {
			return this.tfidf.compareTo(wTFIDF.getTfidf());
		}
	}

}
