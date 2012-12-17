package com.xzg.kmeans.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.xzg.kmeans.io.customtypes.DocumentWordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;

public class R4TFIDFReducer extends
		Reducer<LongWritable, WordTFIDFValue, LongWritable, WordTFIDFValues> {
	@Override
	protected void reduce(LongWritable key, Iterable<WordTFIDFValue> values,
			Context context) throws IOException, InterruptedException {
		WordTFIDFValues wTFIDFValues = new WordTFIDFValues();
		for(WordTFIDFValue value : values){
			WordTFIDFValue wordTFIDFValue = new WordTFIDFValue();
			wordTFIDFValue.setTfidf(value.getTfidf());
			wordTFIDFValue.setWord(value.getWord());
			wTFIDFValues.add(wordTFIDFValue);
			
			
			//需要修改
			//wTFIDFValues.setSize(wTFIDFValues.getSize() + 1);
		}
		context.write(key, wTFIDFValues);
	}

}
