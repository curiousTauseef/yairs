import java.io.*;
import java.lang.String;
import java.lang.System;
import java.util.*;

public class mrgen {
	/**
	 * @param args
	 */
	static String[] a = new String[] {"anchor", "title", "url", "body"};
	static double[] b = new double[] {0.03,	0,	0.02,	0.95};
	static HashMap<String, String> f = new HashMap<String, String>();

    static String fieldSplitter = "+";

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		BufferedReader inf = new BufferedReader(new FileReader("queries.txt"));
		FileWriter outf =new FileWriter("mr_03000295.txt");
		String line;
		while ( (line=inf.readLine()) != null ){
			String[] tmp = line.split("\\(");
			String[] words = tmp[1].substring(0, tmp[1].length()-1).split(" ");
			outf.write(tmp[0]+"(");
			for (String w: words){
				if (!f.containsKey(w)){
					String ans = "#weight( ";					
					for (int i=0; i<a.length; i++)
						ans += b[i] + " " + w + fieldSplitter + a[i] + " ";
					ans += ") ";
					f.put(w, ans);
				}
				outf.write(f.get(w));
			}
			outf.write(")\n");
		}
		outf.close();
		inf.close();
	}
}
