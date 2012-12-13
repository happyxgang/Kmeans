package com.xzg.kmeans.utils;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;
import com.xzg.kmeans.io.inputformat.MR2InputFormat;
import com.xzg.kmeans.mapper.M1WordsPerDocumentMapper;
import com.xzg.kmeans.mapper.M2DocumentWordNumMapper;
import com.xzg.kmeans.reducer.R1WordsPerDocumentReducer;
import com.xzg.kmeans.reducer.R2DocumentWordNumReducer;

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
		Job job = new Job(getConf(), "Kmeans step 1");

		job.setJarByClass(getClass());

		// Format configure
		System.out.println("Input Path : " + args[0]);
		System.out.println("Output Path : " + args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Mapper Reducer Combiner
		job.setMapperClass(M1WordsPerDocumentMapper.class);
		job.setReducerClass(R1WordsPerDocumentReducer.class);
		job.setOutputKeyClass(WordDocumentKey.class);
		job.setOutputValueClass(IntWritable.class);

		Job job2 = new Job(getConf(), "Kmeans step 2");

		job2.setJarByClass(getClass());

		// Format configure
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "2"));

		// Set InputFormat
		job2.setInputFormatClass(MR2InputFormat.class);

		// Mapper Reducer Combiner
		job2.setMapperClass(M2DocumentWordNumMapper.class);
		job2.setReducerClass(R2DocumentWordNumReducer.class);

		// Set Output data type
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(WordWordNumValue.class);

		job2.setOutputKeyClass(WordDocumentKey.class);
		job2.setOutputValueClass(WordNumDocumentWordNumValue.class);

		ControlledJob ctljob1 = new ControlledJob(job, null);

		ControlledJob ctljob2 = new ControlledJob(job2, null);

		JobControl jobControl = new JobControl("Kmeans");
		jobControl.addJob(ctljob1);
		jobControl.addJob(ctljob2);
		ctljob2.addDependingJob(ctljob1);

		Thread jobThread = new Thread(jobControl);

		 jobThread.start();
		while (true) {
			if(jobControl.allFinished() == true){
				System.out.println("JobControl Completed!");
				return 0;
			}
		}
		// return job.waitForCompletion(true) ? 0 : 1;
		// return job2.waitForCompletion(true) ? 0 : 1;

		// // return job.waitForCompletion(true) ? 0 : 1;
		//
		// return 1;
	}

	public static void main(String[] args) throws Exception {
		// Get hdfs configuration
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));

		// Create filesystem
		FileSystem fs = FileSystem.get(conf);

		Path pathreg = new Path("/user/kevin/output*");

		// delete if exist
		FileStatus[] filestatuses = fs.globStatus(pathreg);
		if (filestatuses != null) {
			for (FileStatus filestatus : filestatuses) {
				System.out.println("Deleting File: " + filestatus.getPath());
				fs.delete(filestatus.getPath(), true);
			}
		}

		// Execute Kmeans
		int exitCode = ToolRunner.run(new Kmeans(), args);

		System.out.println("Completed!");

		System.exit(exitCode);
	}
}
