package com.ly.recommend_backend.config;

import lombok.Data;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.Serializable;

@Data
public class ProgramConfig implements Serializable {
    private KafkaSourceConfig kafkaSource;
    private KafkaSinkConfig kafkaSink;
    private ClickhouseConfig clickhouse;
    private MySqlConfig mysql;
    private RedisConfig redis;


    public static ProgramConfig loadFromYaml(InputStream configInputStream) {
        Yaml yaml = new Yaml();
        return yaml.loadAs(configInputStream, ProgramConfig.class);
    }
}
