package com.xzg.kmeans.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.xzg.kmeans.io.customtypes.CustomTextWritable;
import com.xzg.kmeans.io.customtypes.VocabularyValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;

public class R51VocabularyReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// NullWritable nw = new NullWritable();
//		VocabularyValue vocabulary = new VocabularyValue();
//		for (Text t : values) {
//			String str = new String(t.toString());
//			vocabulary.add(str);
//		}
		CustomTextWritable t = new CustomTextWritable();
		t.set(key.toString());
		context.write((Text)t, NullWritable.get());
	}

}
