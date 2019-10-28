package com.example.pocflink;

import com.example.pocflink.service.datastream.WordCount;

public class FlinkApplication {

	public static void main(String[] args) throws Exception {

		// DataSetApi dataSetApi = new DataSetApi();
		// dataSetApi.run(args);

		WordCount wordCount = new WordCount();
		wordCount.run(args);
	}
}