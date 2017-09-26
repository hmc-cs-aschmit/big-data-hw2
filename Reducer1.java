package cs181;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	/* TODO - Implement the reduce function. 
	 * 
	 * 
	 * Input :    Adjacency Matrix Format       ->	( j   ,   M  \t  i	\t value 
	 * 			  Vector Format					->	( j   ,   V  \t   value )
	 * 
	 * Output :   Key-Value Pairs               
	 * 			  Key ->   	i
	 * 			  Value -> 	M_ij * V_j  
	 * 						
	 */

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double vVal = 0;
		ArrayList<String> mList = new ArrayList<String> ();
					
		// Loop through values, to add m_ij term to mList and save v_j to variable v_j
		// Then Iterate through the terms in mList, to multiply each term by variable v_j.
		// Each output is a key-value pair  ( i  ,   m_ij * v_j)

		Text outputKey = new Text();
		Text outputValue = new Text();

		for (Text value : values) {
			String vals  = value.toString();
			String[] indicesAndValue = vals.split("\t");
			
			if (indicesAndValue[0].equals("V")) { // vector
				vVal += Double.parseDouble(indicesAndValue[1]);
			}
			if (indicesAndValue[0].equals("M") ){ // matrix
				mList.add(vals);
			}
		}

		for (String numb : mList) {
			System.out.println(numb);
			String[] indicesAndValue = numb.split("\t");
			outputKey.set(indicesAndValue[1]);
			outputValue.set(Double.toString(vVal*Double.parseDouble(indicesAndValue[2])));
			
			context.write(outputKey, outputValue);
		}	
	}
}
