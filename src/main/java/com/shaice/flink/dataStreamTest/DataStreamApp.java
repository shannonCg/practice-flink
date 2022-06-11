package com.shaice.flink.dataStreamTest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamApp {
    
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
            new Person("Fred", 35),
            new Person("Wilma", 35),
            new Person("Pebbles", 2)
        );

        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {

            @Override
            public boolean filter(Person value) throws Exception {
                return value.getAge() >= 18;
            }
            
        });

        adults.print();
        env.execute();
    }
}
