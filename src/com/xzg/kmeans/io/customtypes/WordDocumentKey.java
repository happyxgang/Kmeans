package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordDocumentKey implements WritableComparable {
	private String word;
	private Long documentNum;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getDocumentNum() {
		return documentNum;
	}

	public void setDocumentNum(Long documentNum) {
		this.documentNum = documentNum;
	}

	public WordDocumentKey() {
		super();
		this.word = new String();
		this.documentNum = 0L;
	}

	public WordDocumentKey(String word, Long documentNum) {
		super();
		this.word = word;
		this.documentNum = documentNum;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word = in.readUTF();
		documentNum = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(word);
		out.writeLong(documentNum);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (o instanceof WordDocumentKey) {
			WordDocumentKey wd = (WordDocumentKey) o;
			if (wd.getWord().equals(this.word)) {
				return this.getDocumentNum().compareTo(wd.getDocumentNum());
			} else {
				return this.getWord().compareTo(wd.getWord());
			}
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return word + "," + documentNum;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		this.getWord();
		return word.hashCode() ^ documentNum.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (!(obj instanceof WordDocumentKey)) {
			return false;
		}

		WordDocumentKey wd = (WordDocumentKey) obj;
		if (this.word.equals(wd.getWord())
				&& this.documentNum.equals(wd.getDocumentNum())) {
			return true;
		} else {
			return false;
		}
	}

}
