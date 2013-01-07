package com.xzg.kmeans.mapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.VectorWritable;

import com.xzg.kmeans.io.customtypes.CustomArrayWritable;
import com.xzg.kmeans.io.customtypes.CustomHashMapWritable;
import com.xzg.kmeans.io.customtypes.VocabularyValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;

public class M52VocabularyMatrixMapper
		extends
		Mapper<LongWritable, WordTFIDFValues, LongWritable, CustomHashMapWritable> {

	// VocabularyValue vocabulary = new VocabularyValue();
	HashMap<String, Double> vocabulary = new HashMap<String, Double>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		// Path path = new Path("output51/");

		// Configuration conf = new Configuration();

		// Path path = new Path("output51");
		// System.out.println(path);
		// FileSystem fs = FileSystem.get(conf);
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));
		//Configuration conf = context.getConfiguration();

		FileSystem fs = FileSystem.get(conf);
		String vectorsPath = "output51/part-r-00000";
		Path path = new Path(vectorsPath);
		// SequenceFile.Reader read = new SequenceFile.Reader(fs, new
		// Path("<path do dictionary>"), conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		VocabularyValue value = new VocabularyValue();
		while (reader.next(NullWritable.get(), value)) {

		}
		for (String str : value.getWords()) {
			vocabulary.put(str, new Double(0.0));
		}
		reader.close();
	}

	// Read in pair of word and tfidf then put into a map to form the matrix
	@Override
	protected void map(LongWritable key, WordTFIDFValues value, Context context)
			throws IOException, InterruptedException {
		Long size = value.getSize();
		HashMap<String, Double> hm = (HashMap<String, Double>) vocabulary
				.clone();
		for (WordTFIDFValue v : value.getValues()) {
			hm.put(v.getWord(), (v.getTfidf()));
			// context.write(key, value)
		}

//		Vector<Double> array = new Vector<Double>();
//		for (Double d : hm.values()) {
//			array.add(d.doubleValue());
//		}
		// ArrayWritable set = new ArrayWritable(DoubleWritable.class);
		// DoubleWritable[] w ;
		// w = new DoubleWritable[hm.values().size()];
		// set.set(hm.values().toArray(w));
		//
//		VectorWritable v = new VectorWritable();
//		v.set(array);
		CustomHashMapWritable c = new CustomHashMapWritable();
		c.add(hm);
		context.write(key, c);
	}

}
