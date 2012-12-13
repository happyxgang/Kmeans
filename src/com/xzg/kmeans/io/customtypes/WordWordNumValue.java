package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordWordNumValue implements WritableComparable {

	String word;
	Integer wordNum;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getWordnum() {
		return wordNum;
	}

	public void setWordnum(int num) {
		this.wordNum = num;
	}

	public WordWordNumValue(String word, int num) {
		this.word = word;
		this.wordNum = num;
	}

	public WordWordNumValue() {
		word = new String();
		wordNum = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.word = in.readUTF();
		this.wordNum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.word);
		out.writeInt(this.wordNum);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		WordWordNumValue wdn = (WordWordNumValue) obj;
		if (this.wordNum == wdn.getWordnum()) {
			return this.word.equals(wdn.word);
		}
		return false;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "word = " + word.toString() + ",num = " + wordNum;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		WordWordNumValue wwn = (WordWordNumValue) o;
		if (this.word.equals(wwn.getWord())) {
			return this.wordNum.compareTo(wwn.getWordnum());
		} else {
			return this.word.compareTo(wwn.getWord());
		}

	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		
		return this.word.hashCode();
	}

}
