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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
//import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
//import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.xzg.kmeans.io.customtypes.DocumentWordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumDocumentNumValue;
import com.xzg.kmeans.io.customtypes.WordNumDocumentWordNumValue;
import com.xzg.kmeans.io.customtypes.WordWordNumValue;
import com.xzg.kmeans.mapper.M1WordsPerDocumentMapper;
import com.xzg.kmeans.mapper.M2DocumentWordNumMapper;
import com.xzg.kmeans.mapper.M3DocumentNumberMapper;
import com.xzg.kmeans.reducer.R1WordsPerDocumentReducer;
import com.xzg.kmeans.reducer.R2DocumentWordNumReducer;
import com.xzg.kmeans.reducer.R3DocumentNumberMapper;

/**
 * Example test of the IdentityMapper to demonstrate proper MapDriver usage in a
 * test case.
 * 
 * This example is reproduced in the overview for the MRUnit javadoc.
 */
// @SuppressWarnings("deprecation")
public class TestExample {

	private Mapper<Object, Text, WordDocumentKey, IntWritable> mapper1;
	private Mapper<WordDocumentKey, IntWritable, LongWritable, WordWordNumValue> mapper2;
	private Reducer<WordDocumentKey, IntWritable, WordDocumentKey, IntWritable> reducer1;
	private Reducer<LongWritable, WordWordNumValue, WordDocumentKey, WordNumDocumentWordNumValue> reducer2;
	private MapDriver<Object, Text, WordDocumentKey, IntWritable> mdriver1;
	private MapDriver<WordDocumentKey, IntWritable, LongWritable, WordWordNumValue> mdriver2;
	private ReduceDriver<WordDocumentKey, IntWritable, WordDocumentKey, IntWritable> rdriver1;
	private ReduceDriver<LongWritable, WordWordNumValue, WordDocumentKey, WordNumDocumentWordNumValue> rdriver2;

	private Mapper<WordDocumentKey, WordNumDocumentWordNumValue, Text, DocumentWordNumDocumentWordNumValue> mapper3;
	private Reducer<Text, DocumentWordNumDocumentWordNumValue, WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> reducer3;
	private MapDriver<WordDocumentKey, WordNumDocumentWordNumValue, Text, DocumentWordNumDocumentWordNumValue> mdriver3;
	private ReduceDriver<Text, DocumentWordNumDocumentWordNumValue, WordDocumentKey, WordNumDocumentWordNumDocumentNumValue> rdriver3;

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

		iOne = new IntWritable(1);
		iTwo = new IntWritable(2);
		iThree = new IntWritable(3);
		lOne = new LongWritable(1L);
		lTwo = new LongWritable(2L);
		lThree = new LongWritable(3L);
	}

	@Test
	public void testM1() {
		mdriver1.withInput(new Text("foo"),
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
		mdriver2.withInput(wd, iTwo)
				.withOutput(lOne, new WordWordNumValue("xzg", 2)).runTest();

	}

	@Test
	public void TestR2() {
		rdriver2.withInput(
				lOne,
				Arrays.asList(new WordWordNumValue("xzg", 2),
						new WordWordNumValue("xzg", 2)))
				.withOutput(wd, new WordNumDocumentWordNumValue(4, 4L))
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
				new WordNumDocumentWordNumValue(2, 4L))
				.withOutput(new Text("xzg"),
						new DocumentWordNumDocumentWordNumValue(1L, 2, 4L))
				.runTest();

	}

	@Test
	public void TestR3() {
		rdriver3.withInput(
				new Text("xzg"),
				Arrays.asList(new DocumentWordNumDocumentWordNumValue(1L, 2, 4L)))
				.withOutput(new WordDocumentKey("xzg", 1L),
						new WordNumDocumentWordNumDocumentNumValue(2, 4L, 1L))
				.runTest();
	}

}