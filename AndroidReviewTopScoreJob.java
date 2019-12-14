package com.group9.application.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

public class AndroidReviewTopScoreJob {

	private final static IntWritable ONE = new IntWritable(1);

	public static void buildJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "TopScore");
		job.setJarByClass(AndroidReviewTopScoreJob.class);
		job.setMapperClass(JSONTopScoreMapper.class);
		job.setCombinerClass(JSONTopScoreReducer.class);
		job.setReducerClass(JSONTopScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

	public static class JSONTopScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] jsonData = value.toString().split("\\n");
			try {
				for (String piece : jsonData) {
					JSONObject jsonObj = new JSONObject(piece);
					double overall = jsonObj.getDouble("overall");
					if (overall == 5.0D) { // Get the Reviews that are only 5.0 - the highest score possible.
						context.write(new Text(String.valueOf(overall)), ONE);
					}
				}

			} catch (JSONException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class JSONTopScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
