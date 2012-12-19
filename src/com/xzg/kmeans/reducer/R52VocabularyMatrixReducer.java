package com.xzg.kmeans.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.xzg.kmeans.io.customtypes.CustomArrayWritable;
import com.xzg.kmeans.io.customtypes.VocabularyValue;

public class R52VocabularyMatrixReducer extends
		Reducer<LongWritable, CustomArrayWritable, NullWritable, VocabularyValue> {

}
