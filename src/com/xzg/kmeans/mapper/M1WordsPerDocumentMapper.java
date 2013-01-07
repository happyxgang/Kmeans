package com.xzg.kmeans.mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import com.xzg.kmeans.io.customtypes.WordDocumentKey;

public class M1WordsPerDocumentMapper extends
		Mapper<LongWritable, Text, WordDocumentKey, IntWritable> {
	public enum DocumentNum {
		DocumentNumCounter
	}

	static WordDocumentKey wd = new WordDocumentKey();
	static IntWritable num = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get the DocumentNum counter and + 1
		Counter cnt = context.getCounter(DocumentNum.DocumentNumCounter);
//		if (cnt.getValue() == 100L) {
//			return;
//		}
		cnt.increment(1);

		// Get the current document number
		long documentNum = key.get();
//		if(documentNum != 34 ){
//			return; 
//		}else {
//			System.out.println("Document 35");
//			System.out.println(value.toString());
//		}
		// Get the Page
		String page = value.toString();
		Document doc = Jsoup.parse(page, "UTF-8");
		
//		if(documentNum != 34){
//			return; 
//		}else {
//			System.out.println(doc.body().text());
//			
//		}
		
		if (doc != null) {
			// Get the Web content
			Element e = doc.body();
			if (e == null) {
				return;
			}
			String content = e.text();
			if (doc.title() != null) {
				content += doc.title() + doc.title()
						+ doc.title();
			}
			
			if (content != null) {
				// Replace punctuations and SBC cases
//				content = content.replaceAll(
//						"\\p{Punct}|[，＂»©…一『』 ￥%……&*——-〜：！·；？‘’“”。、《》（）]|\\d+",
//						"");
				content = content.replaceAll("\\p{Punct}", " ");
				// content = content.replaceAll(
				// "\\p{Punct}|[，＂»©…一『』 ￥%……&*——-〜：！·；？‘’“”。、《》（）]|\\d+", "");

				// ICTCLAS Chinese Word Segment
				// ICTCLAS50 ict = new ICTCLAS50();
				//
				// // Define the configure directory
				// String argu = ".";
				//
				// // Init of ICTCLAS
				// if (!ict.ICTCLAS_Init(argu.getBytes("GB2312"))) {
				// System.out.println("failed!");
				// }
				//
				// // Word Segment
				// byte nativeBytes[] = ict.ICTCLAS_ParagraphProcess(
				// content.getBytes("UTF-8"), 0, 0);
				//
				// // System.out.println(nativeBytes.length);
				// String nativeStr = new String(nativeBytes, 0,
				// nativeBytes.length, "UTF-8");
				//
				// //System.out.println("The result is ：" + nativeStr);
				// ict.ICTCLAS_Exit();
				// String[] words = nativeStr.split("\\p{javaWhitespace}+");
				
				IKSegmenter seg=null;

			
				StringReader reader=new StringReader(content);
				HashMap<String, Integer> hm = new HashMap<String, Integer>();
				seg=new IKSegmenter(reader, true);
				
				Lexeme lex=new Lexeme(0, 0, 0, 0);
				try {
					while((lex=seg.next())!=null)
					{
						String word = lex.getLexemeText();
						if(word.length() < 2){
							
						}
						
						if(word.length() < 7){
							if (hm.containsKey(word)) {
								hm.put(word, hm.get(word) + 1);
							} else {
								hm.put(word, 1);
							}
							//System.out.print(word + "|");
						}
						
					}
				} catch (IOException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
					//return ;
				}
				Iterator itr = hm.entrySet().iterator();
				Entry<String, Integer> entry;
				String word;
				boolean b = true;
				while (itr.hasNext()) {
					entry = (Entry) itr.next();
					word = entry.getKey();
//					if (word.length() > 7) {
//						continue;
//					}
					wd.setDocumentNum(documentNum);
					wd.setWord(word);
					num.set(entry.getValue());
					context.write(wd, num);
					b = false;
				}
				if(b){
					num.set(1);
					context.write(new WordDocumentKey("BadRequest",documentNum), num);
				}
				

			}
		}
	}



}
