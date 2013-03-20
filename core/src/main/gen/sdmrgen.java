package main;

import java.io.*;
import java.util.*;

public class sdmrgen {

	/**
	 * @param args
	 */
	
	static String[] a = new String[] {"anchor", "url", "body", "title"};
	//weight for different representation
	static double[] b = new double[] {0.02598525,	0.002068559,	0.935296093,	0.036650099};
	//weight for different part in sd
	static double w1 = 0.8, w2 = 0.1, w3 = 0.1;
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		BufferedReader inf = new BufferedReader(new FileReader("Indri.query"));
		FileWriter outf =new FileWriter("sdmr.query");
		String line;
		while ( (line=inf.readLine()) != null ){
			String[] tmp = line.split("\\(");
			String[] words = tmp[1].substring(0, tmp[1].length()-1).split(" ");
			String id = tmp[0].split(":")[0];
			outf.write(id + ":#weight( " + w1 + " #and( ");
			for (String w: words){
				String ans = "#weight( ";					
				for (int i=0; i<a.length; i++)
					ans += b[i] + " " + w + "." + a[i] + " ";
				ans += ") ";
				outf.write(ans);
			}
			
			outf.write(") " + w2 + " #and( ");
			for (int i=1; i<words.length; i++){
				outf.write("#weight(");
				for (int j=0; j<a.length; j++)
					outf.write(b[j] + " #near/3(" + words[i-1] + "." + a[j] + " " + words[i] + "." + a[j] + ") ");
				outf.write(")");
			}
			
			outf.write(") " + w3 + " #and( ");
			for (int i=1; i<words.length; i++){
				outf.write("#weight(");
				for (int j=0; j<a.length; j++)
					outf.write(b[j] + " #uw/6(" + words[i-1] + "." + a[j] + " " + words[i] + "." + a[j] + ") ");
				outf.write(")");
			}
			outf.write(") )\n");
		}
		outf.close();
		inf.close();
	}
}
