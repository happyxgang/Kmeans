package com.xzg.kmeans.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;

public class R1WordsPerDocumentReducer extends
		Reducer<WordDocumentKey, IntWritable, WordDocumentKey, IntWritable> {
	public static IntWritable total = new IntWritable(0);
	//public static IntWritable Z = new IntWritable(0);
	@Override
	protected void reduce(WordDocumentKey key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int t = 0;
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()){
			t += it.next().get();
		}
		total.set(t);
		context.write(key, total);
	}
		
}
