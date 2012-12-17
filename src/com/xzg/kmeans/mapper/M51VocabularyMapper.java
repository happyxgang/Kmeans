package com.xzg.kmeans.mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;

public class M51VocabularyMapper 
extends
Mapper<LongWritable, WordTFIDFValues, Text, Text> {

	@Override
	protected void map(LongWritable key, WordTFIDFValues value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HashSet<String> hs = new HashSet<String>();
		Iterator itr = value.getValues().iterator();
		while(itr.hasNext()){
			WordTFIDFValue w =(WordTFIDFValue) itr.next();
			System.out.println(w.getWord());
			context.write(new Text("word"), new Text(w.getWord()));
		}
	}	

}
