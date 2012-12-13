package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordNumDocumentWordNumDocumentNumValue implements
		WritableComparable {


	Integer wordNum;
	Long documentWordNum;
	Long documentNum;

	public Integer getWordNum() {
		return wordNum;
	}

	public void setWordNum(Integer wordNum) {
		this.wordNum = wordNum;
	}

	public Long getDocumentWordNum() {
		return documentWordNum;
	}

	public void setDocumentWordNum(Long documentWordNum) {
		this.documentWordNum = documentWordNum;
	}

	public Long getDocumentNum() {
		return documentNum;
	}

	public void setDocumentNum(Long documentNum) {
		this.documentNum = documentNum;
	}

	public WordNumDocumentWordNumDocumentNumValue(Integer wordNum,
			Long documentWordNum, Long documentNum) {

		this.wordNum = wordNum;
		this.documentWordNum = documentWordNum;
		this.documentNum = documentNum;
	}

	public WordNumDocumentWordNumDocumentNumValue() {
		this.wordNum = 0;
		this.documentWordNum = 0L;
		this.documentNum = 0L;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		WordNumDocumentWordNumDocumentNumValue wndwndn = (WordNumDocumentWordNumDocumentNumValue) o;
		if (this.wordNum.equals(wndwndn.getWordNum())) {
			if (this.documentWordNum.equals(wndwndn.getDocumentWordNum())) {
				return this.documentNum.compareTo(wndwndn.getDocumentNum());
			} else {
				return this.documentWordNum.compareTo(wndwndn
						.getDocumentWordNum());
			}
		} else {
			return this.wordNum.compareTo(wndwndn.getWordNum());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.wordNum = in.readInt();
		this.documentWordNum = in.readLong();
		this.documentNum = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.wordNum);
		out.writeLong(this.documentWordNum);
		out.writeLong(this.documentNum);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.wordNum.hashCode() ^ this.documentWordNum.hashCode()
				^ this.documentNum.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		WordNumDocumentWordNumDocumentNumValue wndwndn = (WordNumDocumentWordNumDocumentNumValue) obj;
		return this.wordNum.equals(wndwndn.getWordNum())
				&& this.documentWordNum.equals(wndwndn.getDocumentWordNum())
				&& this.documentNum.equals(wndwndn.getDocumentNum());
	}
	
	@Override
	public String toString() {
		return "WordNumDocumentWordNumDocumentNumValue [wordNum=" + wordNum
				+ ", documentWordNum=" + documentWordNum + ", documentNum="
				+ documentNum + "]";
	}



}
