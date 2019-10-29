package com.example.pocflink.service.datastream;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

    public void run(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("./data/stream/avg1");

        // month, category,product, profit,
        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter()); // tuple
                                                                                               // [June,Category5,Bat,12]
                                                                                               // [June,Category4,Perfume,10,1]
        mapped.keyBy(0).sum(3).writeAsText("./data/stream/out1").setParallelism(1);

        mapped.keyBy(0).min(3).writeAsText("./data/stream/out2").setParallelism(1);

        mapped.keyBy(0).minBy(3).writeAsText("./data/stream/out3").setParallelism(1);

        mapped.keyBy(0).max(3).writeAsText("./data/stream/out4").setParallelism(1);

        mapped.keyBy(0).maxBy(3).writeAsText("./data/stream/out5").setParallelism(1);
        
        env.execute("Aggregation");
    }
}