-- recs.pp1 definition

CREATE TABLE recs.pp1
(

    `id` UInt8,

    `data` String,

    `etime` Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(etime)
ORDER BY id
SETTINGS index_granularity = 8192
COMMENT '分区表';


-- recs.pp2 definition

CREATE TABLE recs.pp2
(

    `id` UInt8,

    `data` String,

    `etime` Date
)
ENGINE = Log;


-- recs.recs_history_hot_products definition

CREATE TABLE recs.recs_history_hot_products
(

    `product_id` Int32,

    `window_end` Int32,

    `count` String
)
ENGINE = Log
COMMENT '历史热门表';


-- recs.recs_rating definition

CREATE TABLE recs.recs_rating
(

    `user_id` Int32 COMMENT '用户id',

    `product_id` Int32 COMMENT '产品id',

    `score` Float64 COMMENT '评价分数',

    `timestamp` Int32 COMMENT '评价时间'
)
ENGINE = Log
COMMENT '商品评价表';


-- recs.recs_recent_hot_products definition

CREATE TABLE recs.recs_recent_hot_products
(

    `product_id` Int32 COMMENT '产品id，dbeaver不支持填写，通过失去了添加',

    `count` Int32
)
ENGINE = Log
COMMENT '实时热门商品';