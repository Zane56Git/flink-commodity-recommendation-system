package com.ly.recommend_backend.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class KafkaSinkConfig implements Serializable {
    private String bootstrapServer;
    private String topic;
    private String groupId;
    private String plainLoginModule;
    private Integer maxPollRecords;
    private Integer maxPollInterval;


}
