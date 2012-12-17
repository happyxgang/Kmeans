package com.xzg.kmeans.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import ICTCLAS.I3S.AC.ICTCLAS50;

import com.xzg.kmeans.io.customtypes.WordDocumentKey;

public class M1WordsPerDocumentMapper extends
		Mapper<Object, Text, WordDocumentKey, IntWritable> {
	public enum DocumentNum {
		DocumentNumCounter
	}

	static WordDocumentKey wd = new WordDocumentKey();
	static IntWritable num = new IntWritable();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get the DocumentNum counter and + 1
		Counter cnt = context.getCounter(DocumentNum.DocumentNumCounter);
		cnt.increment(1);

		// Get the current document number
		long documentNum = cnt.getValue();

		// Get the Page
		String page = value.toString();
		Document doc = Jsoup.parse(page, "UTF-8");

		// Get the Web content
		String content = doc.body().text();

		// Replace punctuations and SBC cases
		content = content.replaceAll(
				"\\p{Punct}|[，＂»©…一『』 ￥%……&*——-〜：！·；？‘’“”。、《》（）]|\\d+", "");
		// content = content.replaceAll(
		// "\\p{Punct}|[，＂»©…一『』 ￥%……&*——-〜：！·；？‘’“”。、《》（）]|\\d+", "");

		// ICTCLAS Chinese Word Segment
		ICTCLAS50 ict = new ICTCLAS50();

		// Define the configure directory
		String argu = ".";

		// Init of ICTCLAS
		if (!ict.ICTCLAS_Init(argu.getBytes("GB2312"))) {
			System.out.println("failed!");
		} else {
			System.out.println("Init Succeed!");
		}

		// Word Segment
		byte nativeBytes[] = ict.ICTCLAS_ParagraphProcess(
				content.getBytes("UTF-8"), 0, 0);

		// System.out.println(nativeBytes.length);
		String nativeStr = new String(nativeBytes, 0, nativeBytes.length,
				"UTF-8");

		System.out.println("The result is ：" + nativeStr);
		ict.ICTCLAS_Exit();
		String[] words = nativeStr.split("\\p{javaWhitespace}+");

		// Get every word and it's num
		HashMap<String, Integer> hm = new HashMap<String, Integer>();

		for (String word : words) {
			// word = word.trim();
			// if (!word.equals("")) {
			if (hm.containsKey(word)) {
				hm.put(word, hm.get(word) + 1);
			} else {
				hm.put(word, 1);
			}
			// }
		}

		// Set<String> wordset = (Set<String>) hm.keySet();
		// for (String word : wordset) {
		// context.write(new WordDocumentKey(word, documentNum),
		// new IntWritable(hm.get(word)));
		// }
		Iterator itr = hm.entrySet().iterator();
		Entry<String, Integer> entry;
		String word;

		while (itr.hasNext()) {
			entry = (Entry) itr.next();
			word = entry.getKey();
			wd.setDocumentNum(documentNum);
			wd.setWord(word);
			num.set(entry.getValue());
			context.write(wd, num);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);

	}

}
