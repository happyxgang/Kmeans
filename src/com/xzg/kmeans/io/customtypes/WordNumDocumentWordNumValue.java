package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordNumDocumentWordNumValue implements WritableComparable {

	Long wordNum;
	Long documentWordNum;
	
	public Long getWordNum() {
		return wordNum;
	}

	public void setWordNum(Long wordNum) {
		this.wordNum = wordNum;
	}

	public Long getDocumentWordNum() {
		return documentWordNum;
	}

	public void setDocumentWordNum(Long documentWordNum) {
		this.documentWordNum = documentWordNum;
	}

	public WordNumDocumentWordNumValue() {
		wordNum = 0L;
		documentWordNum = 0L;
	}

	public WordNumDocumentWordNumValue(Long wordNum, Long documentNum) {

		this.wordNum = wordNum;
		this.documentWordNum = documentNum;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.wordNum = in.readLong();
		this.documentWordNum = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(wordNum);
		out.writeLong(documentWordNum);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		WordNumDocumentWordNumValue wndn = (WordNumDocumentWordNumValue) obj;
		return (this.wordNum == wndn.getWordNum() && this.documentWordNum == wndn
				.getDocumentWordNum());
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return +this.wordNum + "," + this.documentWordNum;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		WordNumDocumentWordNumValue wndn = (WordNumDocumentWordNumValue) o;
		if (this.wordNum == wndn.getWordNum()) {
			return this.documentWordNum.compareTo(wndn.getDocumentWordNum());
		} else {
			return this.wordNum.compareTo( wndn.getWordNum());
		}
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub

		return this.wordNum.hashCode() ^ this.documentWordNum.hashCode();
	}
}
