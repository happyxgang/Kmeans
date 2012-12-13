package com.xzg.kmeans.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;

public class R2DocumentWordNumReducer
		extends
		Reducer<LongWritable, WordWordNumValue, WordDocumentKey, WordNumDocumentWordNumValue> {
	private WordDocumentKey wd = new WordDocumentKey();
	private WordNumDocumentWordNumValue wndn = new WordNumDocumentWordNumValue();

	@Override
	protected void reduce(LongWritable key, Iterable<WordWordNumValue> values,
			Context context) throws IOException, InterruptedException {
		Long dnums = 0L;

		HashMap<WordDocumentKey, WordNumDocumentWordNumValue> hm = new HashMap<WordDocumentKey, WordNumDocumentWordNumValue>();

		for (WordWordNumValue wwn : values) {
			// Count the document num
			dnums += wwn.getWordnum();
			WordDocumentKey wdtmp = new WordDocumentKey();
			WordNumDocumentWordNumValue wndntmp;
			wdtmp.setWord(wwn.getWord());
			wdtmp.setDocumentNum(key.get());			
			if(hm.containsKey(wdtmp)){
				wndntmp = hm.get(wdtmp);
			}else{
				wndntmp = new WordNumDocumentWordNumValue();
			}
			wndntmp.setWordNum(wndntmp.getWordNum() + wwn.getWordnum());
			wndntmp.setDocumentWordNum(0L);
			
			hm.put(wdtmp, wndntmp);

		}

		Iterator<Entry<WordDocumentKey, WordNumDocumentWordNumValue>> iter = hm
				.entrySet().iterator();
		System.out.println("---------------------");
		
		while (iter.hasNext()) {
			Map.Entry<WordDocumentKey, WordNumDocumentWordNumValue> entry = (Map.Entry) iter
					.next();
			wd = (WordDocumentKey) entry.getKey();
			wndn = (WordNumDocumentWordNumValue) entry.getValue();
			wndn.setDocumentWordNum(dnums);

			context.write(wd, wndn);

		}

	}

}
