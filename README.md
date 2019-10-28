# poc-apache-flink

Brincadeiras iniciais com https://flink.apache.org/

# downloads

https://flink.apache.org/downloads.html

# local run

.\flink run D:\_and\apache-flink\src\pocflink\build\libs\pocflink-0.0.1-SNAPSHOT.jar -input file:///D:/_and/apache-flink/teste -output file:///D:/_and/apache-flink/teste_out


# vscode debug

{
    "configurations": [
        {
            "type": "java",
            "name": "(Launch) - FirstExample",
            "request": "launch",
            "mainClass": "com.example.pocflink.FlinkApplication",
            "projectName": "pocflink",
            "args": "-input file:///D:/_and/apache-flink/teste -output file:///D:/_and/apache-flink/teste_out"
        },
        {
            "type": "java",
            "name": "(Launch) - InnerJoin",
            "request": "launch",
            "mainClass": "com.example.pocflink.FlinkApplication",
            "projectName": "pocflink",
            "args": "-input1 file:///D:/_and/apache-flink/person -input2 file:///D:/_and/apache-flink/location -output file:///D:/_and/apache-flink/innerJoinResult"
        },
        {
            "type": "java",
            "name": "(Launch) - Challenge",
            "request": "launch",
            "mainClass": "com.example.pocflink.FlinkApplication",
            "projectName": "pocflink",
            "args": "-clientes file:///D:/_and/apache-flink/clientes -contratos file:///D:/_and/apache-flink/contratos -parcelas file:///D:/_and/apache-flink/parcelas -output file:///D:/_and/apache-flink/clienteContratoParcela"
        },
        {
            "type": "java",
            "name": "(Launch) - WordCount Stream",
            "request": "launch",
            "mainClass": "com.example.pocflink.FlinkApplication",
            "projectName": "pocflink",
            "args": ""
        }
    ]
}