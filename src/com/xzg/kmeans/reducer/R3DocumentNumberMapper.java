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
import com.xzg.kmeans.io.customtypes.WordWordNumValue;

public class R3DocumentNumberMapper
		extends
		Reducer<Text, DocumentWordNumDocumentWordNumValue, WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> {
	private static WordDocumentKey wd = new WordDocumentKey();
	private static WordNumDocumentWordNumDocumentNumValue wndwndn = new WordNumDocumentWordNumDocumentNumValue();

	@Override
	protected void reduce(Text key,
			Iterable<DocumentWordNumDocumentWordNumValue> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HashMap<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> hm = new HashMap<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue>();
		WordDocumentKey wdTmp = new WordDocumentKey();
		WordNumDocumentWordNumDocumentNumValue wndwndnTmp = new WordNumDocumentWordNumDocumentNumValue();
		Long docs = 0L;
		for (DocumentWordNumDocumentWordNumValue docwndwn : values) {
			docs++;
			wdTmp.setWord(key.toString());
			wdTmp.setDocumentNum(docwndwn.getDocumentId());
			wndwndnTmp.setWordNum(docwndwn.getWordNum());
			wndwndnTmp.setDocumentWordNum(docwndwn.getDocumentWordNum());
			hm.put(wdTmp, wndwndnTmp);
		}
		Iterator itr = hm.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> entry = (Entry<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue>) itr
					.next();
			wd = entry.getKey();
			wndwndn = entry.getValue();
			wndwndn.setDocumentNum(docs);
			context.write(wdTmp, wndwndn);
		}

	}

}
