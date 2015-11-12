package com.example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @author vaibhav tupe
 * ApacheSparkCSVParser Class takes input as CSV file,
 * removes unnecessary spaces and quotes, writes the cleaned
 * content again to CSV file. Also writes the total error count in 
 * CSV file to console.
 *
 */
public class ApcheSparkCSVParser{

	static StringBuilder sb= new StringBuilder();
	static int totalError = 0;
	
	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: ApacheSparkCSVParser <inputFile> <outputFile>");
			System.exit(1);
		}

		String inputFile=args[0];
        String outputFile=args[1];

		SparkConf sparkConf = new SparkConf().setAppName("CSVParser").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines=ctx.textFile(inputFile);
		final String DELIMITER = ",";
		File file = new File(outputFile);
		FileWriter fw=null;
		BufferedWriter bw=null;

		try {
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			lines.foreach(new VoidFunction<String>() {    
				@Override
				public void call(String line) throws Exception {
					// TODO Auto-generated method stub

					String[] tokens = line.split(DELIMITER);
					for(int i=0; i< tokens.length;i++)
					{
						String token = tokens[i];
						int totalLength = token.length();
						token=token.trim();
						token = token.replaceAll("^\"|\"$", "");
						totalError+=(totalLength-token.length());
						sb.append(token);
						if(i<tokens.length-1)
							sb.append(DELIMITER);  
					}
					sb.append(System.getProperty("line.separator"));
				}
			});
			System.out.println("Total Number of Errors : "+totalError);

			bw.write(sb.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				bw.close();
				ctx.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
