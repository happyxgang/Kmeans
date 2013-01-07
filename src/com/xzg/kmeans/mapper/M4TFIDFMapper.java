package com.xzg.kmeans.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;

public class M4TFIDFMapper
		extends
		Mapper<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue, LongWritable, WordTFIDFValue> {

	public enum RunCount {
		M4MapperCount
	}

	@Override
	protected void map(WordDocumentKey key,
			WordNumDocumentWordNumDocumentNumValue value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// Counter counter = context.getCounter(DocumentNum.DocumentNumCounter);
		// Long docNum = counter.getValue();
		LongWritable docId = new LongWritable();
		WordTFIDFValue wTFIDF = new WordTFIDFValue();

		long docNum = context.getConfiguration().getLong("DocumentNumber", 0);
		if (docNum == 0) {
			docNum = 10000L;
			System.out.println("Using Default Counter Value!!!" + docNum);
		}

		// System.out.println(docNum);
		docId.set(key.getDocumentNum());
		Double tfidf = calTFIDF(value, docNum);
		wTFIDF.setWord(key.getWord());
		wTFIDF.setTfidf(tfidf);
		context.write(docId, wTFIDF);

	}

	protected Double calTFIDF(WordNumDocumentWordNumDocumentNumValue wndwndn,
			Long docNum) {

		Double tfidf = new Double(0);
		Double tf = new Double(0);
		Double idf = new Double(0);

		tf = Double.valueOf(wndwndn.getWordNum())
				/ wndwndn.getDocumentWordNum();

		idf = Math.log10(Double.valueOf(docNum) / wndwndn.getDocumentNum());

		tfidf = tf * idf;
		return tfidf;
	}
}
