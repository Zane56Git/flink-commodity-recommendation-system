package com.ly.task.OnlineRecommender;

import com.ly.entity.RatingEntity;
import com.ly.entity.TopEntity;
import com.ly.map.RatingEntityMap;
import com.ly.util.AggCount;
import com.ly.util.HotProducts;
import com.ly.util.Property;
import com.ly.util.WindowResultFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 * 实时推荐任务（消费kafka数据统计热门数据）
 * 窗口统计点击量,存入 hbase
 */
public class TopNProductTask {
    private static final int topCount = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 基于 EventTime 开始
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(Property.getStrValue("redis.host"))
                .build();
        Properties properties = Property.getKafkaProperties("topProduct");
        DataStreamSource<String> datasource = env.addSource(
                new FlinkKafkaConsumer<String>("rating", new SimpleStringSchema(), properties)
        );
        DataStream<RatingEntity> source = datasource.map(new RatingEntityMap());
        // 抽取时间戳，生成 watermark
        DataStream<RatingEntity> timeData = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<RatingEntity>() {
            @Override
            public long extractAscendingTimestamp(RatingEntity ratingEntity) {
                return ratingEntity.getTimestamp() * 1000;
            }
        });
        // 窗口统计点击量，数据集的原因，每一次评分当做一次点击
        DataStream<TopEntity> windowData = timeData.keyBy("productId")
                .timeWindow(Time.minutes(60), Time.minutes(1))
                .aggregate(new AggCount(), new WindowResultFunction());
        DataStream<String> topProducts = windowData.keyBy("windowEnd")
                .process(new HotProducts(topCount));
                //存入 hbase  表 "onlineHot", |rowkey||
        topProducts.print();
        env.execute("Hot Product Task");
    }
}
