// Referenced on http://blog.csdn.net/xx_123_1_rj/article/details/44753677.
//Jacobi Method 2
// -- Shaowei Su

package org.myorg;
	
import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class Jacobi extends Configured {
	
	public static final String CONTROL_I = "\u0009";

	static enum Counters { X_COUNTER }

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

		private int n;
		private double[] P;
		private int iter;

		public void configure(JobConf job) {

			n = job.getInt("n", 0); // get matrix size
			iter = job.getInt("iter", 0); //get iteration numbers
			this.P = new double[n]; 

			for(int i=0; i<n; i++) {
				P[i] = 0;
			}
			/* 
				except for the first iterations, 
				all the other iteration will load the previous results from distributed cache
			*/

			if (iter != 0) {
				Path[] xFiles = new Path[0];
				try {
					xFiles = DistributedCache.getLocalCacheFiles(job);
					for (Path xFile : xFiles) {
						BufferedReader fis = new BufferedReader(new FileReader(xFile.toString()));
				        try {
				            String line = fis.readLine();
				            while(line != null) {
				                String[] tokens = line.trim().split("\\s+");
				                if(tokens.length != 2)
				                    throw new IOException("Input file has improper format");
				                int x = Integer.parseInt(tokens[0]);
				                double val = Double.parseDouble(tokens[1]);
				                P[x] = val;
				                line = fis.readLine();
				            }
				        } finally {
				            fis.close();
				        }
					}
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
			}
		}
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	if(line == null || line.equals("")) 
	    		return ;
	    	String[] values = line.split(" ");
	    	if(values.length < 3)
	    		return ;
	    	int rowindex = Integer.parseInt(values[0]);
	    	int colindex = Integer.parseInt(values[1]);
	    	String elevalue = values[2];
	    	/*
				form key-value pairs like (1, a#1#15), stands for row 1 and column 1 with value 15; 
				also, we can distinguish between matrix a and matrix b according to their column number.

	    	*/
			//System.out.println("[Map] row = "+rowindex+" col = "+colindex+" ele = "+elevalue);
			double ele = 0; //the input element
			double outvalue = 0; //output value

			if (colindex != n) {
				ele = Double.parseDouble(elevalue);
				outvalue = ele * P[colindex];				
			}


	    	if (colindex != n) {
	    		if(colindex == rowindex) {
	    			output.collect(new IntWritable(rowindex), new Text("c#"+Integer.toString(colindex)+"#"+elevalue));
	    			//System.out.println("c# "+Integer.toString(colindex)+" # "+elevalue);
	    		} else {
	    			output.collect(new IntWritable(rowindex), new Text("a#"+Integer.toString(colindex)+"#"+String.valueOf(outvalue)));
	    			//System.out.println("a# "+Integer.toString(colindex)+" # "+String.valueOf(outvalue));
	    		}
	    	} else {
	    		output.collect(new IntWritable(rowindex), new Text("b#"+Integer.toString(colindex)+"#"+elevalue));
	    		//System.out.println("b# "+Integer.toString(colindex)+" # "+elevalue);
	    	}
	    }
	}

	public static class Combiner extends MapReduceBase implements Reducer<IntWritable,Text, IntWritable,Text> {

		private int n;

		public void configure(JobConf job) { 
			n = job.getInt("n", 0); // get matrix size
		}
		/*
			In the combiner part, we will add all the a# value from the same row into sum and then transfer the entire massage to reducer
		*/

		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException {
			double sum = 0;
			int i = 0;
	    	double[] ValA = new double[n];

			for(i=0; i<n; i++) {
				ValA[i] = 0;
			}
	    	double ValB = 0;
	    	double ValC = 0;

	    	while (values.hasNext())
	    	{	
	    		Text line = values.next();
				String value = line.toString();
	    		if (value.startsWith("a#")) {
		    		
		    		StringTokenizer token = new StringTokenizer(value, "#");
		    		String[] temp = new String[3];
		    		int k = 0;
		    		while(token.hasMoreTokens()) {
		    			temp[k] = token.nextToken();
		    			k++;
		    		}
		    		int pos = 0;
		    		pos = Integer.parseInt(temp[1]);
		    		ValA[pos] = Double.parseDouble(temp[2]);
		    		//System.out.println("[Combine] ValA[ "+pos+"] = "+ValA[pos]); 			
	    		} else if (value.startsWith("b#")) {

		    		StringTokenizer token = new StringTokenizer(value, "#");
		    		String[] temp = new String[3];
		    		int k = 0;
		    		while(token.hasMoreTokens()) {
		    			temp[k] = token.nextToken();
		    			k++;
		    		}
		    		ValB = Double.parseDouble(temp[2]);
		    		//System.out.println("[Combine] ValB= "+ValB); 	    			
	    		} else if (value.startsWith("c#")) {

		    		StringTokenizer token = new StringTokenizer(value, "#");
		    		String[] temp = new String[3];
		    		int k = 0;
		    		while(token.hasMoreTokens()) {
		    			temp[k] = token.nextToken();
		    			k++;
		    		}
		    		ValC = Double.parseDouble(temp[2]);
		    		//System.out.println("[Combine] ValC= "+ValC);
	    		}
	    	}

	    	for(i=0; i<(n); i++) {
	    		sum += ValA[i];
	    	}
	    	//System.out.println("[Combine] ValA = "+sum+" ValB = "+ValB+" ValC = "+ValC);
			output.collect(key, new Text(String.valueOf(sum)+"#"+String.valueOf(ValB)+"#"+String.valueOf(ValC)));
		}
	}


	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

		private int iter;
		private int n;
		private double eps;
		private double[] P;

		public void configure(JobConf job) {

			n = job.getInt("n", 0); //get matrix size
			iter = job.getInt("iter", 0); //get iteration numbers
			eps = Double.valueOf(job.getStrings("eps", "1e-10")[0]); //get eps

			this.P = new double[n]; 

			for(int i=0; i<n; i++) {
				P[i] = 0;
			}
			/* 
				except for the first iterations, 
				all the other iteration will load the previous results from distributed cache
			*/

			if (iter != 0) {
				Path[] xFiles = new Path[0];
				try {
					xFiles = DistributedCache.getLocalCacheFiles(job);
					for (Path xFile : xFiles) {
						BufferedReader fis = new BufferedReader(new FileReader(xFile.toString()));
				        try {
				            String line = fis.readLine();
				            while(line != null) {
				                String[] tokens = line.trim().split("\\s+");
				                if(tokens.length != 2)
				                    throw new IOException("Input file has improper format");
				                int x = Integer.parseInt(tokens[0]);
				                double val = Double.parseDouble(tokens[1]);
				                P[x] = val;
				                line = fis.readLine();
				            }
				        } finally {
				            fis.close();
				        }
					}
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}

			}

		}
	    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	double ValA = 0;
	    	double ValB = 0;
	    	double ValC = 0;

	    	Integer rowInd;
	    	rowInd = key.get();

	    	while (values.hasNext())
	    	{	
	    		Text line = values.next();
				String value = line.toString();

		    		StringTokenizer token = new StringTokenizer(value, "#");
		    		String[] temp = new String[3];
		    		int k = 0;
		    		while(token.hasMoreTokens()) {
		    			temp[k] = token.nextToken();
		    			k++;
		    		}
		    		ValA = Double.parseDouble(temp[0]);
		    		ValB = Double.parseDouble(temp[1]);
		    		ValC = Double.parseDouble(temp[2]); 			

	    	}

	    	double x_new = (ValB - ValA) / ValC;
	    	output.collect(key, new Text(String.valueOf(x_new)));
	    	//System.out.println("[Reduce] ValA = "+ValA+" ValB = "+ValB+" ValC = "+ValC);

	    	/*
				check if it meets requirement
	    	*/
	    	if (Math.abs(x_new-P[rowInd]) > eps) {
	    		reporter.incrCounter(Counters.X_COUNTER, 1);
	    	}
	    }
	}
	
	public static void main(String[] args) throws Exception {

	    int iter = 0; //record current iteration
        int n; //the size of matrix
        long counter = 1;  //the counter used to detect how many x finished computation
        double eps = 1e-10;
        int max_iter = 100;
        long main_start=System.currentTimeMillis();

        int prefix = args[0].lastIndexOf('/') + 1;
        int suffix = args[0].lastIndexOf('.');
        n = Integer.parseInt(args[0].substring(prefix, suffix));

	    while (iter< max_iter) {
	    	if (counter == 0) {
	    		break; // end of iterating
	    	}
		    JobConf conf = new JobConf(Jacobi.class);
		    conf.setJobName("jacobi" + Integer.toString(iter));
		
			conf.setInt("iter", iter);
			conf.setInt("n", n);
			conf.setStrings("eps", Double.toString(eps));
			conf.setInt("max_iter", max_iter);

		    conf.setOutputKeyClass(IntWritable.class);
		    conf.setOutputValueClass(Text.class);
		
		    conf.setMapperClass(Map.class);
		    conf.setCombinerClass(Combiner.class);
		    conf.setReducerClass(Reduce.class);
		
		    conf.setInputFormat(TextInputFormat.class);
		    conf.setOutputFormat(TextOutputFormat.class);
		    conf.setNumMapTasks(1);
		    conf.setNumReduceTasks(1);

		    /*
				read in previous output to distributed cache
		    */
		    if (iter != 0) {
		    	Path xOut = new Path(args[1]+"/"+Integer.toString(iter-1)+"/part-00000");
		    	DistributedCache.addCacheFile(xOut.toUri(), conf);
		    }
		
		    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		    FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/"+Integer.toString(iter)));
		
		    RunningJob lastJob = JobClient.runJob(conf);
		    counter = lastJob.getCounters().getCounter(Counters.X_COUNTER);

		    System.out.println("Now finish " + iter + " iterations");

		    iter++;

	    }
	    long main_end=System.currentTimeMillis();
	    System.out.println("Total execution time:"+ " :  "+(main_end - main_start) +"  ms");
	    System.exit(0);
	}
}
