package com.dw.OI;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Reducer;

public class OrderInversion {

	public static class PairOfWords implements WritableComparable<PairOfWords>{

		private String left;
		public void setLeft(String left) {
			this.left = left;
		}

		public void setRight(String right) {
			this.right = right;
		}

		private String right;
		
		public String getLeft() {
			return left;
		}

		public String getRight() {
			return right;
		}
		
		public PairOfWords() {
			
		}
		
		public void set(String left, String right) {
			this.left = left;
			this.right = right;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(left);
			out.writeUTF(right);
		}

		public void readFields(DataInput in) throws IOException {
			left = in.readUTF();
			right = in.readUTF();
		}

		public int compareTo(PairOfWords o) {
			//排序，有*的排到前面，使其可以先算总数
			int returnVal = left.compareTo(o.getLeft());
			if(returnVal != 0) {
				return returnVal;
			}
			if(right.equals("*")) {
				return -1;
			}else if(o.getRight().equals("*")) {
				return 1;
			}
			return right.compareTo(o.getRight());
		}
		
		public boolean equals(Object object) {
			if(object == null)
				return false;
			if(this == object)
				return true;
			if(object instanceof PairOfWords) {
				PairOfWords o = (PairOfWords) object;
				return left.compareTo(o.left) == 0 && right.compareTo(o.right) == 0;
			}else {
				return false;
			}
		}
		
		public String toString() {
			return "(" + left + " , " + right + ")";
		}
	}
	
	public static class OrderInversionMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable>{
		private int neighborwindow = 2;
		private final PairOfWords pair = new PairOfWords();
		private final IntWritable totalCount = new IntWritable();
		private final IntWritable one = new IntWritable(1);
		
		public void setup(Context context) {
			//System.out.println("------------------------setup");
			this.neighborwindow = context.getConfiguration().getInt("neighbor.window", 2);
		}
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("------------------------map-----------");
			String[] tokens = StringUtils.split(value.toString(), " ");
			//System.out.println("tokens.size: " + tokens.length);
			if((tokens == null) || (tokens.length < 2)) {
				return;
			}
			for(int i=0; i<tokens.length; i++) {
				String word = tokens[i];
	            pair.setLeft(word);
	            int start = 0;
	            if (i - neighborwindow >= 0) {
	                start = i - neighborwindow;
	            }
	            int end = 0;
	            if (i + neighborwindow >= tokens.length) {
	                end = tokens.length - 1;
	            } else {
	                end = i + neighborwindow;
	            }

	            for (int j = start; j <= end; j++) {
	                if (i == j) {
	                    continue;
	                }
	                pair.setRight(tokens[j]);
	                context.write(pair, one);

	            }
	            pair.setRight("*");
	            totalCount.set(end - start);
	            context.write(pair, totalCount);
			}
		}
	}
	
	public static class OrderInversionGroupingComparator extends WritableComparator{
		protected OrderInversionGroupingComparator() {
			super(PairOfWords.class, true);
			System.out.println("GroupingComparator---------------------------------");
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			PairOfWords apair = (PairOfWords) a;
			PairOfWords bpair = (PairOfWords) b;
	
			int mini = apair.getLeft().compareTo(bpair.getLeft());
			if(mini == 0) {
				mini = apair.getRight().compareTo(bpair.getRight());
			}
			return mini;
		}
	}
	
	//减轻Reducer的输入
	public static class OrderInversionCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable>{
		protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int partialSum = 0;
			for(IntWritable val : values) {
				partialSum += val.get();
			}
			context.write(key, new IntWritable(partialSum));
		}
	}
	
	public static class OrderInversionReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable>{
		private double totalCount = 0;
		private final DoubleWritable relativeCount = new DoubleWritable();
		private String currentWord = "NOT_DEFINED";
		
		private int getTotalCount(Iterable<IntWritable> values) {
			int sum = 0;
			for(IntWritable val : values) {
				System.out.println(val.get());
				sum += val.get();
			}
			return sum;
		}
		
		protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("key :" + key);
			if(key.getRight().equals("*")) {
				if(key.getLeft().equals(currentWord)) {
					totalCount += getTotalCount(values);
				}else {
					currentWord = key.getLeft();
					totalCount = getTotalCount(values);
				}
			}else {
				int count = getTotalCount(values);
				relativeCount.set((double)count / totalCount);
				context.write(key, relativeCount);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(true);
		int neighborWindow = Integer.parseInt("2");
		String inputPath = "hdfs://localhost:9000/user/dw/input/infor.txt";
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String outputPath = "hdfs://localhost:9000/user/dw/mr-OI-" + time;
 
        Job job = new Job(conf, "OrderInversion");
        job.setJarByClass(OrderInversion.class);  
  
        job.getConfiguration().setInt("neighbor.window", neighborWindow);  
  
        FileInputFormat.setInputPaths(job, new Path(inputPath));  
        FileOutputFormat.setOutputPath(job, new Path(outputPath));  
  
        // (key,value) generated by map()  
        job.setMapOutputKeyClass(PairOfWords.class);  
        job.setMapOutputValueClass(IntWritable.class);  
  
        // (key,value) generated by reduce()  
        job.setOutputKeyClass(PairOfWords.class);  
        job.setOutputValueClass(DoubleWritable.class);  
  
        job.setMapperClass(OrderInversionMapper.class);  
        job.setReducerClass(OrderInversionReducer.class); 
        // 分组函数
        job.setGroupingComparatorClass(OrderInversionGroupingComparator.class);
        
        //组合函数
        //job.setCombinerClass(OrderInversionCombiner.class);   

        // 提交job
        if (job.waitForCompletion(false)) {
            System.out.println("job ok !");
        } else {
            System.out.println("job error !");
        }
	}
}
