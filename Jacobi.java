// Referenced on http://blog.csdn.net/xx_123_1_rj/article/details/44753677.
// Jacobi method 1
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

		public void configure(JobConf job) {

			n = job.getInt("n", 0); // get matrix size
		}
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	if(line == null || line.equals("")) 
	    		return ;
	    	String[] values = line.split(" ");
	    	if(values.length < 3)
	    		return ;
	    	int rowindex = Integer.parseInt(values[0]);
	    	String colindex = values[1];
	    	String elevalue = values[2];
	    	/*
				form key-value pairs like (1, a#1#15), stands for row 1 and column 1 with value 15; 
				also, we can distinguish between matrix a and matrix b according to their column number.

	    	*/
	    	if (Integer.parseInt(colindex) != n) {
	    		output.collect(new IntWritable(rowindex), new Text("a#"+colindex+"#"+elevalue));
	    	} else {
	    		output.collect(new IntWritable(rowindex), new Text("b#"+colindex+"#"+elevalue));
	    	}
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
	    	double[] ValA = new double[n];
	    	double ValB = 0;

	    	Integer rowInd;
	    	rowInd = key.get();


	    	/*
				To separate the value of a[][] and b[] from the text input
	    	*/
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
	    		} else if (value.startsWith("b#")) {

		    		StringTokenizer token = new StringTokenizer(value, "#");
		    		String[] temp = new String[3];
		    		int k = 0;
		    		while(token.hasMoreTokens()) {
		    			temp[k] = token.nextToken();
		    			k++;
		    		}
		    		ValB = Double.parseDouble(temp[2]);	    			
	    		}
	    	}

	    	/* 
				compute one single element of x[]
	    	*/
	    	double sum = 0;
	    	double x_new = 0;
	    	for(int j = 0; j < n; j++) 
	    		if (j != rowInd) 
	    			sum -= ValA[j] * P[j];
	    	sum += ValB;

	    	x_new = sum/ValA[rowInd];
	    	output.collect(key, new Text(String.valueOf(x_new)));

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
		    conf.setReducerClass(Reduce.class);
		
		    conf.setInputFormat(TextInputFormat.class);
		    conf.setOutputFormat(TextOutputFormat.class);

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
