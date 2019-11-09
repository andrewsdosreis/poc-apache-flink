package com.example.pocflink;

import com.example.pocflink.service.window.GlobalProcessing;

public class FlinkApplication {

	public static void main(String[] args) throws Exception {

		// DataSetApi process = new DataSetApi();
		
		/* Stream Processing */
		// WordCount process = new WordCount();
		// Reduce process = new Reduce();
		// Iterate process = new Iterate();
		// Split process = new Split();
		// Aggregation process = new Aggregation();

		/* Window Assigned */
		// Challenge process = new Challenge();
		// TumblingEvent process = new TumblingEvent();
		//TumblingProcessing process = new TumblingProcessing();

		GlobalProcessing process = new GlobalProcessing();

		process.run(args);
	}
}