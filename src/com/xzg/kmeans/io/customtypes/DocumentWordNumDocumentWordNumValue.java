package com.xzg.kmeans.io.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DocumentWordNumDocumentWordNumValue implements WritableComparable {
	Long documentId;
	Long wordNum;
	Long documentWordNum;

	public Long getDocumentId() {
		return documentId;
	}

	public void setDocumentId(Long documentId) {
		this.documentId = documentId;
	}

	public Long getWordNum() {
		return wordNum;
	}

	public void setWordNum(Long wordNum) {
		this.wordNum = wordNum;
	}

	public Long getDocumentWordNum() {
		return documentWordNum;
	}

	public void setDocumentWordNum(Long documentNum) {
		this.documentWordNum = documentNum;
	}

	public DocumentWordNumDocumentWordNumValue() {
		this.documentId = 0L;
		this.wordNum = 0L;
		this.documentWordNum = 0L;
	}

	public DocumentWordNumDocumentWordNumValue(Long documentId,
			Long wordNum, Long documentNum) {
		super();
		this.documentId = documentId;
		this.wordNum = wordNum;
		this.documentWordNum = documentNum;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		DocumentWordNumDocumentWordNumValue docwndwn = (DocumentWordNumDocumentWordNumValue) o;
		if (this.documentId.equals(docwndwn.getDocumentId())) {

			if (this.wordNum.equals(docwndwn.getWordNum())) {

				return this.documentWordNum.compareTo(docwndwn
						.getDocumentWordNum());
			} else {
				return this.wordNum.compareTo(docwndwn.getWordNum());
			}
		} else {
			return this.documentId.compareTo(docwndwn.getDocumentId());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.documentId = in.readLong();
		this.wordNum = in.readLong();
		this.documentWordNum = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.documentId);
		out.writeLong(this.wordNum);
		out.writeLong(this.documentWordNum);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.documentId.hashCode() ^ this.wordNum.hashCode()
				^ this.documentWordNum.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		DocumentWordNumDocumentWordNumValue dwndwn = (DocumentWordNumDocumentWordNumValue) obj;
		return (this.documentId.equals(dwndwn.getDocumentId())
				&& this.wordNum.equals(dwndwn.getWordNum()) 
				&& this.documentWordNum.equals(dwndwn.getDocumentWordNum()));
	}

	@Override
	public String toString() {
		return " [documentId=" + documentId
				+ ", wordNum=" + wordNum + ", documentWordNum="
				+ documentWordNum + "]";
	}

	

}
