package co.nubetech.hiho.testdata;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;

public class NGramLogic {
	public static void main(String[] args) {
		new NGramLogic().getNGrams(new Text("This is a book"), 2);
	}	

	public HashSet<String> getNGrams(Text line, int gramSize) {
		ArrayList<String> words = new ArrayList<String>();
		HashSet<String> nGrams = new HashSet<String>();
		String[] tokens = line.toString().split(" ");
		for (String t : tokens) {
			words.add(t);
		}
		for (int i = 0; i < words.size() - gramSize + 1; i++) {
			String key = "";
			for (int j = i; j < i + gramSize; j++) {
				key += words.get(j);
				if(j != ( i + gramSize - 1)){
				key += " ";
				}
			}
			nGrams.add(key);
		}
		for (String ngram : nGrams) {
			System.out.println(ngram);
		}
		return nGrams;
	}

	public void end() {
		
	}
}
