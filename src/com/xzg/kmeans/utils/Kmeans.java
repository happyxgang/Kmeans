package com.xzg.kmeans.utils;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import com.xzg.kmeans.io.customtypes.CustomArrayWritable;
import com.xzg.kmeans.io.customtypes.CustomHashMapWritable;
import com.xzg.kmeans.io.customtypes.DocumentWordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.VocabularyValue;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;
import com.xzg.kmeans.io.inputformat.MR2InputFormat;
import com.xzg.kmeans.io.inputformat.MR3InputFormat;
import com.xzg.kmeans.io.inputformat.MR4InputFormat;
import com.xzg.kmeans.mapper.M1WordsPerDocumentMapper;
import com.xzg.kmeans.mapper.M1WordsPerDocumentMapper.DocumentNum;
import com.xzg.kmeans.mapper.M2DocumentWordNumMapper;
import com.xzg.kmeans.mapper.M3DocumentNumberMapper;
import com.xzg.kmeans.mapper.M4TFIDFMapper;
import com.xzg.kmeans.mapper.M51VocabularyMapper;
import com.xzg.kmeans.mapper.M52VocabularyMatrixMapper;
import com.xzg.kmeans.reducer.R1WordsPerDocumentReducer;
import com.xzg.kmeans.reducer.R2DocumentWordNumReducer;
import com.xzg.kmeans.reducer.R3DocumentNumberMapper;
import com.xzg.kmeans.reducer.R4TFIDFReducer;
import com.xzg.kmeans.reducer.R51VocabularyReducer;

public class Kmeans extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// Check args
		if (args.length != 2) {
			System.err.printf("Usage:%s [genetic options] <input> <output> \n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job1 = new Job(getConf(), "Kmeans step 1");

		job1.setJarByClass(getClass());

		job1.setInputFormatClass(SequenceFileInputFormat.class);

		// Format configure
		System.out.println("Input Path : " + args[0]);
		System.out.println("Output Path : " + args[1]);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "1"));

		// Mapper Reducer Combiner
		job1.setMapperClass(M1WordsPerDocumentMapper.class);
		job1.setReducerClass(R1WordsPerDocumentReducer.class);
		// job1.setCombinerClass()

		job1.setOutputKeyClass(WordDocumentKey.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		// job1.waitForCompletion(true);

		// Counters counters = job1.getCounters();
		// Counter c = counters.findCounter(DocumentNum.DocumentNumCounter);
		long docNum = 10000;
		// docNum = c.getValue();

		Job job2 = new Job(getConf(), "Kmeans step 2");

		job2.setJarByClass(getClass());

		// Format configure
		FileInputFormat.addInputPath(job2, new Path(args[1] + "1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "2"));

		// Set InputFormat
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		// Mapper Reducer Combiner
		job2.setMapperClass(M2DocumentWordNumMapper.class);
		job2.setReducerClass(R2DocumentWordNumReducer.class);

		// Set Output data type
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(WordWordNumValue.class);

		job2.setOutputKeyClass(WordDocumentKey.class);
		job2.setOutputValueClass(WordNumDocumentWordNumValue.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		Job job3 = new Job(getConf(), "Kmeans step 3");

		job3.setJarByClass(getClass());

		// Format configure

		FileInputFormat.addInputPath(job3, new Path(args[1] + "2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "3"));

		// job3.setInputFormatClass(MR3InputFormat.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);

		// Mapper Reducer Combiner
		job3.setMapperClass(M3DocumentNumberMapper.class);
		job3.setReducerClass(R3DocumentNumberMapper.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DocumentWordNumDocumentWordNumValue.class);

		job3.setOutputKeyClass(WordDocumentKey.class);
		job3.setOutputValueClass(WordNumDocumentWordNumDocumentNumValue.class);

		job3.setOutputFormatClass(SequenceFileOutputFormat.class);

		// SequenceFileOutputFormat.setCompressOutput(job3, true);
		// SequenceFileOutputFormat.setOutputCompressorClass(job3,
		// GzipCodec.class);
		// SequenceFileOutputFormat.setOutputCompressionType(job3,
		// CompressionType.BLOCK);
		//

		// // System.out.println("Starting M4");
		Configuration configuration = getConf();

		configuration.setLong("DocumentNumber", docNum);
		Job job4 = new Job(configuration, "Kmeans step 4");

		job4.setJarByClass(getClass());

		// Format configure

		FileInputFormat.addInputPath(job4, new Path(args[1] + "3"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "4test"));

		job4.setInputFormatClass(SequenceFileInputFormat.class);

		job4.setMapperClass(M4TFIDFMapper.class);
		job4.setReducerClass(R4TFIDFReducer.class);

		job4.setMapOutputKeyClass(LongWritable.class);
		job4.setMapOutputValueClass(WordTFIDFValue.class);

		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(WordTFIDFValues.class);

		job4.setOutputFormatClass(SequenceFileOutputFormat.class);
		
	
		
		//
		Job job51 = new Job(getConf(), "Kmeans step 51");

		job51.setJarByClass(getClass());

		// Format configure

		FileInputFormat.addInputPath(job51, new Path(args[1] + "4"));
		FileOutputFormat.setOutputPath(job51, new Path(args[1] + "51"));

		job51.setInputFormatClass(SequenceFileInputFormat.class);

		job51.setMapperClass(M51VocabularyMapper.class);
		job51.setReducerClass(R51VocabularyReducer.class);

		job51.setMapOutputKeyClass(Text.class);
		job51.setMapOutputValueClass(NullWritable.class);

		job51.setOutputKeyClass(Text.class);
		job51.setOutputValueClass(NullWritable.class);
		job51.setOutputFormatClass(SequenceFileOutputFormat.class);

		// job51.setOutputKeyClass(Text.class);
		// job51.setOutputValueClass(Text.class);

		Job job52 = new Job(getConf(), "Kmeans step 52");

		job52.setJarByClass(getClass());

		// Format configure

		FileInputFormat.addInputPath(job52, new Path(args[1] + "4"));
		FileOutputFormat.setOutputPath(job52, new Path(args[1] + "52"));

		job52.setInputFormatClass(SequenceFileInputFormat.class);

		job52.setMapperClass(M52VocabularyMatrixMapper.class);
		// job52.setReducerClass(R51VocabularyReducer.class);

		job52.setMapOutputKeyClass(LongWritable.class);
		job52.setMapOutputValueClass(CustomArrayWritable.class);

		job52.setOutputKeyClass(LongWritable.class);
		job52.setOutputValueClass(CustomHashMapWritable.class);

		// ????????????????????????????????????
		job52.setNumReduceTasks(0);

		//job52.setOutputFormatClass(SequenceFileOutputFormat.class);

		 //job2.waitForCompletion(true);
		// job3.waitForCompletion(true);
		
		//job4.waitForCompletion(true);
		job51.waitForCompletion(true);
		 //job52.waitForCompletion(true);

		// ControlledJob ctljob1 = new ControlledJob(job1, null);
		//
		// ControlledJob ctljob2 = new ControlledJob(job2, null);
		// ControlledJob ctljob3 = new ControlledJob(job3, null);
		// ControlledJob ctljob4 = new ControlledJob(job4, null);
		// ControlledJob ctljob51 = new ControlledJob(job51, null);
		// ControlledJob ctljob52 = new ControlledJob(job52, null);
		//
		// JobControl jobControl = new JobControl("Kmeans");
		//
		// jobControl.addJob(ctljob1);
		// jobControl.addJob(ctljob2);
		// jobControl.addJob(ctljob3);
		// jobControl.addJob(ctljob4);
		// jobControl.addJob(ctljob51);
		// jobControl.addJob(ctljob52);
		//
		// ctljob2.addDependingJob(ctljob1);
		// ctljob3.addDependingJob(ctljob2);
		//
		// ctljob4.addDependingJob(ctljob3);
		// ctljob51.addDependingJob(ctljob4);
		// ctljob52.addDependingJob(ctljob51);
		//
		// Thread jobThread = new Thread(jobControl);
		//
		// jobThread.start();
		// boolean b = true;
		// while (b) {
		// if (jobControl.allFinished() == true) {
		// System.out.println("JobControl Completed!");
		// //return 0;
		// break;
		// }
		// }
		// Configuration conf = new Configuration();
		// conf.addResource(new
		// Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));

//		System.out.println("InputDriver Running");
//		//
		InputDriver.runJob(new Path(
				"hdfs://localhost:9000/user/kevin/output52/part-m-00000"),
				new Path("hdfs://localhost:9000/user/kevin/mahout/tfidfdata"),
				"org.apache.mahout.math.SequentialAccessSparseVector");

		// System.out.println("InputDriver Completed");
		// System.out.println("CanopyDriver Running");
		//
		// CanopyDriver.run(conf, new Path("mahout/tfidfdata"), new Path(
		// "mahout/canopy"), new ManhattanDistanceMeasure(), (float) 3.1,
		// (float) 2.1, false, 0.001, false);
		//
		// System.out.println("CanopyDriver Stopped");
		// System.out.println("KmeansDriver Running");
		//

//		Path samples = new Path(
//				"hdfs://localhost:9000/user/kevin/mahout/tfidfdata/");
//		// 创建一个空的数据文件夹
//		Path output = new Path("hdfs://localhost:9000/user/kevin/mahout/");
//		// HadoopUtil.delete(getConf(), output);
//		// 创建距离度量方式(这里使用的是余弦距离)
//		DistanceMeasure measure = new CosineDistanceMeasure();
//
//		Path clustersIn = new Path(output, "random-seeds");
//		// 根据原始数据samples获得随即的3个聚类中心
//		RandomSeedGenerator.buildRandom(getConf(), samples, clustersIn, 3,
//				measure);

		// // // now run the KMeansDriver job
		//
		// KMeansDriver.run(getConf(), new Path(
		// "hdfs://localhost:9000/user/kevin/mahout/tfidfdata"), new Path(
		// "hdfs://localhost:9000/user/kevin/mahout/random-seeds"),
		// new Path("hdfs://localhost:9000/user/kevin/mahout/kmeans"),
		// new CosineDistanceMeasure(), (double) 0.001, 10, true, 0.001,
		// false);
		// // ClusterDumper clusterDumper = new ClusterDumper(
		// // new
		// Path("hdfs://localhost:9000/user/kevin/output7/clusteredPoints"),
		// // new Path(new Path("hdfs://localhost:9000/user/kevin/output8"),
		// // "clusteredPoints"));
		// // clusterDumper.printClusters(null);
		//
		
//		 Configuration conf = new Configuration();
//		 conf.addResource(new
//				 Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));
//		 FileSystem fs = FileSystem.get(conf);
//		
//		 Path path = new Path(
//		 "hdfs://localhost:9000/user/kevin/mahout/kmeans/clusteredPoints/part-m-00000");
//		 Path path2 = new Path(
//		 "hdfs://localhost:9000/user/kevin/mahout/kmeans/clusters-1/part-r-00000");
//		 Path path3 = new Path(
//		 "hdfs://localhost:9000/user/kevin/mahout/random-seeds/part-randomSeed");
//		 Reader reader = new SequenceFile.Reader(fs, path3, getConf());
//		
//		 
//		 // Configuration()); // 获得Key，也就是之前写入的userId
//		 Text key = new Text();
//		 ClusterWritable value = new ClusterWritable();
//		 // WeightedVectorWritable value = new WeightedVectorWritable();
//		 while (reader.next(key, value)) {
//		
//		 System.out.print(key + "   ");
//		
//		 Cluster c = value.getValue();
//		 System.out.println("Cluster ID : " + c.getId());
//		 }

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Get hdfs configuration
		// Configuration conf = new Configuration();
		// conf.addResource(new
		// Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));

		// Create filesystem
		FileSystem fs = FileSystem.get(new Configuration());
		//
		// Path pathreg = new Path("output*");
		// // delete if exist
		// FileStatus[] filestatuses = fs.globStatus(pathreg);
		// if (filestatuses != null) {
		// for (FileStatus filestatus : filestatuses) {
		// System.out.println("Deleting File: " + filestatus.getPath());
		// fs.delete(filestatus.getPath(), true);
		// }
		// }
		//
		// Path pathreg2 = new Path("output51/part*");
		// // delete if exist
		// FileStatus[] filestatuses2 = fs.globStatus(pathreg2);
		// if (filestatuses2 != null) {
		// for (FileStatus filestatus : filestatuses2) {
		// // System.out.println("Deleting File: " + filestatus.getPath());
		// // fs.delete(filestatus.getPath(), true);
		// }
		// }
		// Execute Kmeans
		int exitCode = ToolRunner.run(new Kmeans(), args);

		System.out.println("Completed!");

		// System.exit(exitCode);
	}



}
