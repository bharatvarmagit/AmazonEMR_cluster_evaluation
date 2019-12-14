package com.group9.application.project;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;


public class UserReviewDominateScore {
	
	public static void buildJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "TopScore");
		job.setJarByClass(UserReviewDominateScore.class);
		job.setMapperClass(UserReviewDominateScoreMapper.class);
		job.setCombinerClass(UserReviewDominateScoreReducer.class);
		job.setReducerClass(UserReviewDominateScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class UserReviewDominateScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] jsonData = value.toString().split("\\n");
			try {
				for (String piece : jsonData) {
					JSONObject jsonObj = new JSONObject(piece);
					String reviewerId = jsonObj.getString("reviewerID");
					double overall = jsonObj.getDouble("overall");
					// Creates the reviewerId and overall property k-v pairs
					context.write(new Text(reviewerId), new Text(String.valueOf(overall)));
				}

			} catch (JSONException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class UserReviewDominateScoreReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Map<String, Integer> map = MapUtility.buildKeywordTotalMap(values);
			int total = 0; // Placeholder for max occurences
			String keyword = ""; // Placeholder word
			for (String piece : map.keySet()) {
				if (map.get(piece) > total) {
					total = map.get(piece);
					keyword = piece; // Reassign
				}
			}
			context.write(key, new Text(keyword));
		}
	}
}
