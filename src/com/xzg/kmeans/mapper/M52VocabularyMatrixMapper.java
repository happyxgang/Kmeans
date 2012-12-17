package com.xzg.kmeans.mapper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.xzg.kmeans.io.customtypes.VocabularyValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;

public class M52VocabularyMatrixMapper extends
Mapper<LongWritable, WordTFIDFValues, LongWritable, ArrayWritable> {
	
	//VocabularyValue vocabulary = new VocabularyValue();
	HashMap<String,Double> hm = new HashMap<String, Double>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		//Path path = new Path("output51/");
		Configuration conf = context.getConfiguration();
	    Path path = new Path(conf.get("output51/"));
	    FileSystem fs = FileSystem.get(conf);

	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		VocabularyValue value = new VocabularyValue();
		while(reader.next(NullWritable.get(), value)){
			
		}
		for(String str : value.getWords()){
			hm.put(str, 0.0);
		}
		reader.close();
	}

	
	@Override
	protected void map(LongWritable key,  WordTFIDFValues value,
			Context context)
			throws IOException, InterruptedException {
		Long size = value.getSize();
		for(WordTFIDFValue v : value.getValues()){
			hm.put(v.getWord(), v.getTfidf());
		}
		
	}

}
