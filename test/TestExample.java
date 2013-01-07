/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
//import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.xzg.kmeans.io.customtypes.DocumentWordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValue;
import com.xzg.kmeans.io.customtypes.WordTFIDFValues;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;
import com.xzg.kmeans.mapper.M1WordsPerDocumentMapper;
import com.xzg.kmeans.mapper.M2DocumentWordNumMapper;
import com.xzg.kmeans.mapper.M3DocumentNumberMapper;
import com.xzg.kmeans.mapper.M4TFIDFMapper;
import com.xzg.kmeans.mapper.M4TFIDFMapper.RunCount;
import com.xzg.kmeans.reducer.R1WordsPerDocumentReducer;
import com.xzg.kmeans.reducer.R2DocumentWordNumReducer;
import com.xzg.kmeans.reducer.R3DocumentNumberMapper;
import com.xzg.kmeans.reducer.R4TFIDFReducer;

/**
 * Example test of the IdentityMapper to demonstrate proper MapDriver usage in a
 * test case.
 * 
 * This example is reproduced in the overview for the MRUnit javadoc.
 */
// @SuppressWarnings("deprecation")
public class TestExample {

	private Mapper<LongWritable, Text, WordDocumentKey, IntWritable> mapper1;
	private Mapper<WordDocumentKey, IntWritable, LongWritable, WordWordNumValue> mapper2;
	private Reducer<WordDocumentKey, IntWritable, WordDocumentKey, IntWritable> reducer1;
	private Reducer<LongWritable, WordWordNumValue, WordDocumentKey, WordNumDocumentWordNumValue> reducer2;
	private MapDriver<LongWritable, Text, WordDocumentKey, IntWritable> mdriver1;
	private MapDriver<WordDocumentKey, IntWritable, LongWritable, WordWordNumValue> mdriver2;
	private ReduceDriver<WordDocumentKey, IntWritable, WordDocumentKey, IntWritable> rdriver1;
	private ReduceDriver<LongWritable, WordWordNumValue, WordDocumentKey, WordNumDocumentWordNumValue> rdriver2;

	private Mapper<WordDocumentKey, WordNumDocumentWordNumValue, Text, DocumentWordNumDocumentWordNumValue> mapper3;
	private Reducer<Text, DocumentWordNumDocumentWordNumValue, WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> reducer3;
	private MapDriver<WordDocumentKey, WordNumDocumentWordNumValue, Text, DocumentWordNumDocumentWordNumValue> mdriver3;
	private ReduceDriver<Text, DocumentWordNumDocumentWordNumValue, WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> rdriver3;

	private Mapper<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue, LongWritable, WordTFIDFValue> mapper4;
	private Reducer<LongWritable, WordTFIDFValue, LongWritable, WordTFIDFValues> reducer4;
	private MapDriver<WordDocumentKey, WordNumDocumentWordNumDocumentNumValue, LongWritable, WordTFIDFValue> mdriver4;
	private ReduceDriver<LongWritable, WordTFIDFValue, LongWritable, WordTFIDFValues> rdriver4;

	private WordDocumentKey wd;
	private IntWritable iOne;
	private IntWritable iTwo;
	private IntWritable iThree;
	private LongWritable lOne;
	private LongWritable lTwo;
	private LongWritable lThree;

	@Before
	public void setUp() {
		wd = new WordDocumentKey("xzg", 1L);

		mapper1 = new M1WordsPerDocumentMapper();
		mapper2 = new M2DocumentWordNumMapper();

		reducer1 = new R1WordsPerDocumentReducer();
		reducer2 = new R2DocumentWordNumReducer();

		mdriver1 = MapDriver.newMapDriver(mapper1);
		mdriver2 = MapDriver.newMapDriver(mapper2);

		rdriver1 = ReduceDriver.newReduceDriver(reducer1);
		rdriver2 = ReduceDriver.newReduceDriver(reducer2);

		mapper3 = new M3DocumentNumberMapper();
		reducer3 = new R3DocumentNumberMapper();

		mdriver3 = MapDriver.newMapDriver(mapper3);
		rdriver3 = ReduceDriver.newReduceDriver(reducer3);

		mapper4 = new M4TFIDFMapper();
		reducer4 = new R4TFIDFReducer();

		mdriver4 = MapDriver.newMapDriver(mapper4);
		rdriver4 = ReduceDriver.newReduceDriver(reducer4);

		iOne = new IntWritable(1);
		iTwo = new IntWritable(2);
		iThree = new IntWritable(3);
		lOne = new LongWritable(1L);
		lTwo = new LongWritable(2L);
		lThree = new LongWritable(3L);
	}

	@Test
	public void testM1() {
		mdriver1.withInput(new LongWritable(1L),
				new Text("<body>sd(*^^&*%**((& </body>")).withOutput(
				new WordDocumentKey("sd", 1L), new IntWritable(1));

	}

	@Test
	public void TestR1() {
		rdriver1.withInput(wd,
				Arrays.asList(new IntWritable(1), new IntWritable(2)))
				.withOutput(wd, new IntWritable(3)).runTest();
	}

	@Test
	public void TestM2() {
		mdriver2.withInput(wd, iTwo).withInput(wd, iTwo)
				.withOutput(lOne, new WordWordNumValue("xzg", 2))
				.withOutput(lOne, new WordWordNumValue("xzg", 2)).runTest();

	}

	@Test
	public void TestR2() {
		rdriver2.withInput(
				lOne,
				Arrays.asList(new WordWordNumValue("xzg", 2),
						new WordWordNumValue("xzg", 2)))
				.withOutput(wd, new WordNumDocumentWordNumValue(4L, 4L))
				.runTest();
	}

	@Test
	public void TestMR() {
		// final List<Pair<Mapper, Reducer>> pipeline = new
		// ArrayList<Pair<Mapper, Reducer>>();
		// pipeline.add(new Pair(mapper1, reducer1));
		// pipeline.add(new Pair(mapper2, reducer2));
		final PipelineMapReduceDriver<Object, Text, WordDocumentKey, IntWritable> pdriver = PipelineMapReduceDriver
				.newPipelineMapReduceDriver();
		// pdriver.withMapReduce(mapper1, reducer1)
		// .withMapReduce(mapper2, reducer2)
		// .withInput(new Text(""),new Text("<body>xzg </body>"))
		// .withoutput(wd,new WordNumDocumentNumValue(1,1L)).runTest();
	}

	@Test
	public void TestM3() {
		mdriver3.withInput(new WordDocumentKey("xzg", 1L),
				new WordNumDocumentWordNumValue(2L, 4L))
				.withOutput(new Text("xzg"),
						new DocumentWordNumDocumentWordNumValue(1L, 2L, 4L))
				.runTest();

	}

	@Test
	public void TestR3() {
		rdriver3.withInput(
				new Text("xzg"),
				Arrays.asList(new DocumentWordNumDocumentWordNumValue(1L, 2L,
						4L),
						new DocumentWordNumDocumentWordNumValue(2L, 2L, 4L)))
				.withOutput(new WordDocumentKey("xzg", 1L),
						new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 2L))
				.withOutput(new WordDocumentKey("xzg", 2L),
						new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 2L))
				.runTest();
	}

	@Test
	public void TestM4() {
		mdriver4.withInput(new WordDocumentKey("xzg", 1L),
				new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 100L))
				// .withInput(new WordDocumentKey("xzg2", 1L),
				// new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 100L))
				// .withInput(new WordDocumentKey("xzg3", 1L),
				// new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 100L))
				// .withInput(new WordDocumentKey("xzg", 2L),
				// new WordNumDocumentWordNumDocumentNumValue(2L, 4L, 2L))
				// .withOutput(new LongWritable(1L),new
				// WordTFIDFValues(Arrays.asList(new
				// WordTFIDFValue("xzg",0.5))))
				.withOutput(new LongWritable(1L),
						new WordTFIDFValue("xzg", 1.0))
				.withOutput(new LongWritable(1L),
						new WordTFIDFValue("xzg2", 1.0)).runTest();
		Counters cs = mdriver4.getCounters();
		Counter c = cs.findCounter(RunCount.M4MapperCount);
		Long l = c.getValue();
		System.out.println(l);
	}

	@Test
	public void TestR4() {
		rdriver4.withInput(
				lOne,
				Arrays.asList(new WordTFIDFValue("xzg11", 0.5),
						new WordTFIDFValue("xzg11", 0.5)))
				.withOutput(
						lOne,
						new WordTFIDFValues(new PriorityQueue(Arrays.asList(
								new WordTFIDFValue("xzg11", 0.5),
								new WordTFIDFValue("xzg11", 0.5))))).runTest();
	}

	@Test
	public void TestRead4() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(
				"hdfs://localhost:9000/user/kevin/output4test/part-r-00000");
		Reader reader = new SequenceFile.Reader(fs, path, new Configuration());
		// key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), new
		// Configuration()); // 获得Key，也就是之前写入的userId
		LongWritable key = new LongWritable();
		WordTFIDFValues value = new WordTFIDFValues();
		while (reader.next(key, value)) {

			System.out.print(key + "   ");
			System.out.println(value);
			TestExample.write("/home/kevin/Desktop/output", key + " " + value);
		}

	}

	@Test
	public void TestRead3() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop-1.0.4/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(
				"hdfs://localhost:9000/user/kevin/output3/part-r-00000");
		Reader reader = new SequenceFile.Reader(fs, path, new Configuration());
		// key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), new
		// Configuration()); // 获得Key，也就是之前写入的userId
		CustomTextWritable key = new CustomTextWritable();
		WordNumDocumentWordNumDocumentNumValue value = new WordNumDocumentWordNumDocumentNumValue();
		while (reader.next(key, value)) {

			System.out.print(key + "   ");
			System.out.println(value);
			TestExample.write("/home/kevin/Desktop/output", key + " " + value);
		}

	}
	@Test
	public void TestWrite(){
		
	}
	public static void write(String path, String content) {
		String s = new String();
		String s1 = new String();
		try {
			File f = new File(path);
			if (f.exists()) {
				// System.out.println("文件存在");
			} else {
				System.out.println("文件不存在，正在创建...");
				if (f.createNewFile()) {
					System.out.println("文件创建成功！");
				} else {
					System.out.println("文件创建失败！");
				}

			}
			BufferedReader input = new BufferedReader(new FileReader(f));

			while ((s = input.readLine()) != null) {
				s1 += s + "\n";
			}
			input.close();
			s1 += content;

			BufferedWriter output = new BufferedWriter(new FileWriter(f));
			output.write(s1);
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}