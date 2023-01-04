package com.alibaba.ververica.cep.demo;

import com.alibaba.ververica.cep.demo.condition.MiddleCondition;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.TimeBehaviour;
//import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.alibaba.ververica.cep.demo.condition.EndCondition;
import com.alibaba.ververica.cep.demo.condition.StartCondition;
import com.alibaba.ververica.cep.demo.event.Event;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;

public class CepDemo {
//
//    public static void printTestPattern(Pattern<?, ?> pattern) throws JsonProcessingException {
//        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
//    }

    public static void checkArg(String argName, MultipleParameterTool params) {
        if (!params.has(argName)) {
            throw new IllegalArgumentException(argName + " must be set!");
        }
    }

    public static void main(String[] args) throws Exception {
        // Process args
//        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
//        checkArg(KAFKA_BROKERS_ARG, params);
//        checkArg(INPUT_TOPIC_ARG, params);
//        checkArg(INPUT_TOPIC_GROUP_ARG, params);
//        checkArg(JDBC_URL_ARG, params);
//        checkArg(TABLE_NAME_ARG, params);
//        checkArg(JDBC_INTERVAL_MILLIS_ARG, params);

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build Kafka source with new Source API based on FLIP-27
//        KafkaSource<Event> kafkaSource =
//                KafkaSource.<Event>builder()
//                        .setBootstrapServers(params.get(KAFKA_BROKERS_ARG))
//                        .setTopics(params.get(INPUT_TOPIC_ARG))
//                        .setStartingOffsets(OffsetsInitializer.latest())
//                        .setGroupId(params.get(INPUT_TOPIC_GROUP_ARG))
//                        .setDeserializer(new EventDeSerializationSchema())
//                        .build();
//        // DataStream Source
//        DataStreamSource<Event> source =
//                env.fromSource(
//                        kafkaSource,
//                        WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner((event, ts) -> event.getEventTime()),
//                        "Kafka Source");

        env.setParallelism(1);

        // keyBy userId and productionId
        // Notes, only events with the same key will be processd to see if there is a match
//        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
//                source.keyBy(
//                        new KeySelector<Event, Tuple2<Integer, Integer>>() {
//
//                            @Override
//                            public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
//                                return Tuple2.of(value.getId(), value.getProductionId());
//                            }
//                        });

        // show how to print test pattern in json format
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new StartCondition())
                        .followedBy("middle")
                        .where(new MiddleCondition())
                        .followedBy("end")
                        .where(new EndCondition());
//        printTestPattern(pattern);

        // Dynamic CEP patterns
//        SingleOutputStreamOperator<String> output =
//                CEP.dynamicPatterns(
//                        keyedStream,
//                        new JDBCPeriodicPatternProcessorDiscovererFactory<>(
//                                params.get(JDBC_URL_ARG),
//                                JDBC_DRIVE,
//                                params.get(TABLE_NAME_ARG),
//                                null,
//                                Long.parseLong(params.get(JDBC_INTERVAL_MILLIS_ARG))),
//                        TimeBehaviour.ProcessingTime,
//                        TypeInformation.of(new TypeHint<String>() {}));
//        // Print output stream in taskmanager's stdout
//        output.print();

        DataStream<Event> input =
                env.fromElements(
                        new Event(2, "start", 0, 1, 0L, "{ \"group\": \"A\", \"detail\": {\"price\": 12300}}"),
                        new Event(3, "start", 0, 1, 1L, "{ \"group\": \"A\", \"detail\": {\"price\": 12300}}"),
                        new Event(2, "middle", 0, 1, 2L, "{ \"group\": \"AA\", \"detail\": {\"price\": 12300}}"),
                        new Event(3, "middle", 0, 1, 3L, "{ \"group\": \"AA\", \"detail\": {\"price\": 12300}}"),
                        new Event(2, "end", 0, 1, 4L, "{ \"group\": \"A\", \"detail\": {\"price\": 12300}}"),
                        new Event(3, "end", 0, 1, 5L, "{ \"group\": \"A\", \"detail\": {\"price\": 12300}}"),
                        new Event(4, "end", 0, 1, 15L, "{ \"group\": \"A\", \"detail\": {\"price\": 12300}}"));
        input = input.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Event>() {

            @Override
            public long extractTimestamp(
                    Event element, long currentTimestamp) {
                return element.getEventTime();
            }

            @Override
            public Watermark checkAndGetNextWatermark(
                    Event lastElement,
                    long extractedTimestamp) {
                return new Watermark(lastElement.getEventTime() - 5);
            }
        } );

        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                input
                        .keyBy(
                new KeySelector<Event, Tuple2<Integer, Integer>>() {

                    @Override
                    public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
                        return Tuple2.of(value.getId(), value.getProductionId());
                    }
                });

        DataStream<String> result =
                CEP.pattern(keyedStream, pattern)
                        .inEventTime()
                        .flatSelect(
                                (p, o) -> {
                                    StringBuilder builder = new StringBuilder();

                                    builder.append(p.get("start").get(0).getId())
                                            .append(",")
                                            .append(p.get("middle").get(0).getId())
                                            .append(",")
                                            .append(p.get("end").get(0).getId());

                                    o.collect(builder.toString());
                                },
                                Types.STRING);
        result.print();
        // Compile and submit the job
        env.execute("CEPDemo");
    }
}
