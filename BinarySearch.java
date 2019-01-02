import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BinarySearch extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private Text word = new Text();
		
		
		public static double[] merge(double[] a1, double[] a2) {
			int len1 = a1.length;
			int len2 = a2.length;
			int len = len1 + len2;
			
			double[] result = new double[len];
			
			int i= 0; 
			int j = 0;
			int k = 0;
			
			
			while(k < len) {
				
				if(i < len1 && j < len2 && a1[i] <= a2[j]) {
					result[k] = a1[i];
					i++;
					k++;
				}
				else if(i < len1 && j < len2 && a1[i] > a2[j]) {
					result[k] = a2[j];
					j++;
					k++;
				}
				
				else if(i >= len1 && j<len2) {
					result[k] = a2[j];
					j++;
					k++;
				}
				else if(j>= len2 && i < len1) {
					result[k] = a1[i];
					i++;
					k++;
				}
				else {
					break;
				}
				
			}
			return result;
		}		
		
		public static double[] mergesort(double[] arr) {
			
			int len = arr.length;
			
			if(len == 1) {
				return arr;
			}
			
			int prelen = len/2;
			int postlen = len - prelen;
			
			double[] prearr = new double[prelen];
			double[] postarr = new double[postlen];
			
			for (int i=0;i<prelen;i++) {
				
				prearr[i] = arr[i];
				
			}
			
			for(int j=0;j<postlen;j++) {
				
				postarr[j] = arr[prelen+j];
				
			}
			
			double[] sortedpre = mergesort(prearr);
			double[] sortedpost = mergesort(postarr);
			
			return merge(sortedpre,sortedpost);
			
			
			
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			String searchString = conf.get("word");
			double targetvalue = Double.parseDouble(searchString);
			StringTokenizer tokens = new StringTokenizer(line);
			String[] numstring = new String[tokens.countTokens()];
			
			int leng = tokens.countTokens();
			
			int itt = 0;
						
            while (tokens.hasMoreTokens()) {
                String token = tokens.nextToken();
                numstring[itt] = token;
                itt += 1;

            }
            
            int llen = numstring.length;
            
            
			// binary search starts
			if (llen > 0) {

				double[] numsu = new double[numstring.length];

				for (int i = 0; i < numstring.length; i++) {

					numsu[i] = Double.parseDouble(numstring[i]);

				}
				
				
				// merge-sort 
				
				double[] nums = mergesort(numsu);
				
				
				
				

				int l = 0;
				int u = nums.length - 1;

				while (l < u) {
					int m = (l + u) / 2;

					if (targetvalue <= nums[m]) {
						u = m;
					} 
					else {
						l = m + 1;
					}

				}
				
//				System.out.println(nums[l]);
				
				if (targetvalue == nums[l]) {
					context.write(new Text(Double.toString(targetvalue)), one);


				} 
				else {
					word.set(Double.toString(targetvalue));
					context.write(word, zero);
				}

			}

		}
	}
	
    public static class Reduce extends
    Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            int s = 0;
            for (IntWritable value : values) {
                s += value.get();
            }
            
            if (s > 0) {
            	s = 1;
            }
            context.write(key, new IntWritable(s));
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int final_res = ToolRunner.run(conf, new BinarySearch(), args);
		System.exit(final_res);

	}

	@Override 
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.out.printf("Help: 3 arguments are needed for Distributed Binary Search <input directory> <output directory> <target Double Value> \n");
			System.exit(-1);
		}

		String input_file = args[0];
		String output_file = args[1];
		String target_double_string = args[2];
		Configuration conf = new Configuration();
		conf.set("word", target_double_string);
		Job job = new Job(conf, "Binary Search");
		
		
		job.setJarByClass(BinarySearch.class);
		FileSystem fsystem = FileSystem.get(conf);

		Path in = new Path(input_file);
		Path out = new Path(output_file);
		
		if(!fsystem.exists(in)) 
		{
			System.out.println("Please provide a valid input file.");
			System.exit(-1);

		}
		
		if (fsystem.exists(out)) {
			fsystem.delete(out, true);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
	}
}