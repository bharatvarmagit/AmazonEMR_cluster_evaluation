package com.group9.application.project;

import java.io.IOException;

public class Runner {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// Total score collector
		AndroidReviewScoreCounter.buildJob(args);
		// Top Score collector - only collects Review Scores that were 5.0
		AndroidReviewTopScoreJob.buildJob(args);
		// User Score hierarchy - makes a ranking of a user's review scores
		UserReviewDominateScore.buildJob(args);

	}

}
