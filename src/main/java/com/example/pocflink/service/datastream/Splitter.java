package com.example.pocflink.service.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>> {
    
    private static final long serialVersionUID = 2884425186926187641L;

    // 01-06-2018,June,Category5,Bat,12
    public Tuple4<String, String, String, Integer> map(String value) { 
        
        // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
        String[] words = value.split(",");
        
        // ignore timestamp, we don't need it for any calculations
        return new Tuple4<String, String, String, Integer>(words[1], words[2], words[3], Integer.parseInt(words[4]));
    } // June Category5 Bat 12
}