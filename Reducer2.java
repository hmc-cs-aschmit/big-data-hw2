package cs181;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	
	
	/* TODO - Implement the reduce function. This function receives terms for 1 row of Mv multiplication. 
	 * The function should iterate through the 'values' variable and sum-up the terms. 
	 * 
	 * Input :    Key-Value Pair   (i  ,   values => essentially a list of terms [m_i1*v_1, m_i2*v_2, ...] ) 
	 * 
	 * Output :   Key-Value Pairs               
	 * 			  Key ->   	i
	 * 			  Value -> 	sum of terms in 'values'
	 */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		// TODO 
		Text outputKey = key;
		Text outputValue = new Text();
		
		double valueSum = 0;
		for (Text value : values) {
			Double valDoub = Double.parseDouble(value.toString());
			valueSum += valDoub;
		}
		outputValue.set(Double.toString(valueSum));
		context.write(outputKey, outputValue);
				
	}
}

