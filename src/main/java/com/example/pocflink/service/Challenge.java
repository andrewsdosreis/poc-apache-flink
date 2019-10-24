package com.example.pocflink.service;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;

public class Challenge {

    public void run(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> clienteSet = env.readTextFile(params.get("clientes"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });

        // Read location file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> contratoSet = env.readTextFile(params.get("contratos"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });

        DataSet<Tuple2<String, Integer>> parcelaSet = env.readTextFile(params.get("parcelas"))
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple2<String, Integer>(words[0], Integer.parseInt(words[1]));
                    }
                });

        // joined format will be <id, cliente_nome, contrato>
        DataSet<Tuple3<Integer, String, String>> clienteContrato = clienteSet.join(contratoSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> cliente,
                            Tuple2<Integer, String> contrato) {
                        return new Tuple3<Integer, String, String>(cliente.f0, cliente.f1, contrato.f1);
                    }
                });

        // joined format will be <id, cliente_nome, contrato, parcela>
        DataSet<Tuple4<Integer, String, String, Integer>> clienteContratoParcela = clienteContrato.join(parcelaSet)
                .where(2).equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, String, String>, Tuple2<String, Integer>, Tuple4<Integer, String, String, Integer>>() {
                    public Tuple4<Integer, String, String, Integer> join(
                            Tuple3<Integer, String, String> clienteContrato, Tuple2<String, Integer> parcela) {
                        return new Tuple4<Integer, String, String, Integer>(clienteContrato.f0, clienteContrato.f1,
                                clienteContrato.f2, parcela.f1);
                    }
                }).sortPartition(0, Order.ASCENDING);

        clienteContratoParcela.writeAsCsv(params.get("output"), "\n", " ").setParallelism(1);

        env.execute("Challenge");
    }
}