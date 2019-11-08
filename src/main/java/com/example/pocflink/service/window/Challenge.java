package com.example.pocflink.service.window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Challenge {

    public void run(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> clientsData = env.socketTextStream("localhost", 9997);
        DataStream<String> contractsData = env.socketTextStream("localhost", 9998);

        DataStream<Tuple2<String, String>> clients = clientsData.map(new Splitter());
        DataStream<Tuple2<String, String>> contracts = contractsData.map(new Splitter());

        DataStream<Tuple3<String, String, String>> clientContracts = clients
                                                                        .join(contracts)
                                                                        .where(new NameKeySelector())
                                                                        .equalTo(new NameKeySelector())
                                                                        .window(TumblingEventTimeWindows.of(Time.milliseconds(30000)))
                                                                        //.window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                                                                        .apply (new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>() {
                                                                                        @Override
                                                                                        public Tuple3<String, String, String> join(Tuple2<String, String> cliente, Tuple2<String, String> contrato) {
                                                                                            return new Tuple3<String, String, String>(cliente.f0, cliente.f1, contrato.f1);
                                                                                        }
                                                                                    }
                                                                                );

        
        clients.writeAsCsv("./data/challenge/clients_output", WriteMode.NO_OVERWRITE, "\n", " ").setParallelism(1);
        contracts.writeAsCsv("./data/challenge/contracts_output", WriteMode.NO_OVERWRITE, "\n", " ").setParallelism(1);

        clientContracts.writeAsCsv("./data/challenge/output", WriteMode.NO_OVERWRITE, "\n", " ").setParallelism(1);

        env.execute("Streaming Challenge");
    }

    public static class Splitter implements MapFunction<String, Tuple2<String, String>> {

        public Tuple2<String, String> map(String value) {
            String[] words = value.split(",");
            return new Tuple2<String, String>(words[0], words[1]);
        }
    }

	private static class NameKeySelector implements KeySelector<Tuple2<String, String>, String> {
		@Override
		public String getKey(Tuple2<String, String> value) {
			return value.f0;
		}
	}
}