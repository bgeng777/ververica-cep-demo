package com.alibaba.ververica.cep.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.alibaba.ververica.cep.demo.condition.EndCondition;
import com.alibaba.ververica.cep.demo.condition.MiddleCondition;
import com.alibaba.ververica.cep.demo.condition.StartCondition;
import com.alibaba.ververica.cep.demo.event.Event;

public class CepDemo {

    public static void main(String[] args) throws Exception {

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new StartCondition())
                        .followedBy("middle")
                        .where(new MiddleCondition())
                        .followedBy("end")
                        .where(new EndCondition());

        DataStream<Event> input =
                env.fromElements(
                        new Event(2, "start", 0, 1, 0L, "dummy"),
                        new Event(3, "start", 0, 1, 1L, "dummy"),
                        new Event(2, "middle", 0, 1, 2L, "dummy"),
                        new Event(3, "middle", 0, 1, 3L, "dummy"),
                        new Event(2, "end", 0, 1, 4L, "dummy"),
                        new Event(3, "end", 0, 1, 5L, "dummy"),
                        new Event(4, "end", 0, 1, 15L, "dummy"));
        input =
                input.assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<Event>() {

                            @Override
                            public long extractTimestamp(Event element, long currentTimestamp) {
                                return element.getEventTime();
                            }

                            @Override
                            public Watermark checkAndGetNextWatermark(
                                    Event lastElement, long extractedTimestamp) {
                                return new Watermark(lastElement.getEventTime() - 5);
                            }
                        });

        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                input.keyBy(
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
