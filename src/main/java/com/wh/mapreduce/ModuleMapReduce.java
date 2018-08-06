package com.wh.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author wang
 *
 */
public class ModuleMapReduce extends Configured implements Tool {

	// step 1: Map Class
	public static class ModuleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// TODO

		@Override
		public void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// TODO

		}

		@Override
		public void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}

	// step 2: Reduce Class
	public static class ModuleReduer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// TODO

		@Override
		public void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO
		}

		@Override
		public void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context arg0)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.run(arg0);
		}

	}

	// step 3: Driver, component jod
	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1: get configuration

		// 2: create job
		Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());

		// 3: set job
		// input -> map -> reduce ->output

		// run jar
		job.setJarByClass(this.getClass());

		// 3.1 : input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// 3.2 map
		job.setMapperClass(ModuleMapper.class);
		// TODO
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 3.3 reduce
		job.setReducerClass(ModuleReduer.class);
		// TODO
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 3.4 output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		// 4: submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	// step 4: run program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, //
				new ModuleMapReduce(), //
				args);
		System.exit(exitCode);
	}
}
