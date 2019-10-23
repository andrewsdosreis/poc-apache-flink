package com.example.pocflink.service;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class FirstExample {

    public void run(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("N");
            }
        });

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", "").setParallelism(1);
        }

        env.execute("WordCount Example");
    }
}