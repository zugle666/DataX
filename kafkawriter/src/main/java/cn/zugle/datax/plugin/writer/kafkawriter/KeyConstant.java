package cn.zugle.datax.plugin.writer.kafkawriter;

/**
 * @author zhuzhq 2020-11-13
 */
public class KeyConstant {

    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "127.0.0.1:9092";

    public static final String TOPIC = "topic";
    public static final String TOPIC_DEFAULT = "datax_kafka_topic";

    public static final String ACKS = "acks";
    public static final String ACKS_DEFAULT = "all";

    public static final String RETRIES = "retries";
    public static final String RETRIES_DEFAULT = "0";

    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_SIZE_DEFAULT = "16384";

    public static final String LINGER_MS = "lingerMs";
    public static final String LINGER_MS_DEFAULT = "10";

    public static final String BUFFER_MEMORY = "bufferMemory";
    // 32MB
    public static final String BUFFER_MEMORY_DEFAULT = "33554432";

    public static final String KEY_DESERIALIZER = "keyDeserializer";
    public static final String KEY_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String VALUE_DESERIALIZER = "valueDeserializer";
    public static final String VALUE_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String COLUMN = "column";
    public static final String COLUMN_DEFAULT = "[]";

    public static final String PARTITIONS = "partitions";
    public static final int PARTITIONS_DEFAULT = 1;

    public static final String KEYS = "keys";
}
