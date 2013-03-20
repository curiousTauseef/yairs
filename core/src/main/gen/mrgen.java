package main;

import java.io.*;
import java.util.*;

public class mrgen {

	/**
	 * @param args
	 */
	static String[] a = new String[] {"anchor", "url", "body", "title"};
	static double[] b = new double[] {0.02598525,	0.002068559,	0.935296093,	0.036650099};
	static HashMap<String, String> f = new HashMap<String, String>();
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		BufferedReader inf = new BufferedReader(new FileReader("Indri.query"));
		FileWriter outf =new FileWriter("mr.query");
		String line;
		while ( (line=inf.readLine()) != null ){
			String[] tmp = line.split("\\(");
			String[] words = tmp[1].substring(0, tmp[1].length()-1).split(" ");
			outf.write(tmp[0]+"(");
			for (String w: words){
				if (!f.containsKey(w)){
					String ans = "#weight( ";					
					for (int i=0; i<a.length; i++)
						ans += b[i] + " " + w + "." + a[i] + " ";
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
