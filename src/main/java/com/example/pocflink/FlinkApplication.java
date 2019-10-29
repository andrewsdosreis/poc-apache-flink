package com.example.pocflink;

import com.example.pocflink.service.datastream.Aggregation;

public class FlinkApplication {

	public static void main(String[] args) throws Exception {

		// DataSetApi dataSetApi = new DataSetApi();
		// dataSetApi.run(args);

		// WordCount wordCount = new WordCount();
		// wordCount.run(args);

		// Reduce reduce = new Reduce();
		// reduce.run(args);

		// Iterate iterate = new Iterate();
		// iterate.run(args);

		// Split split = new Split();
		// split.run(args);

		Aggregation aggregation = new Aggregation();
		aggregation.run(args);		
	}
}