package cn.zugle.datax.plugin.reader.kafkareader;

import org.apache.kafka.common.serialization.StringSerializer;
/**
 * @author zhuzhq 2020-11-13
 */
public class KeyConstant {

    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "127.0.0.1:9092";

    public static final String TOPIC = "topic";
    public static final String TOPIC_DEFAULT = "datax_kafka_topic";

    public static final String GROUP_ID = "groupId";
    public static final String GROUP_ID_DEFAULT = "datax_default";

    public static final String KEY_DESERIALIZER = "keyDeserializer";
    public static final String KEY_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String VALUE_DESERIALIZER = "valueDeserializer";
    public static final String VALUE_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String ENABLE_AUTO_COMMIT = "enableAutoCommit";
    public static final String ENABLE_AUTO_COMMIT_DEFAULT = "true";

    public static final String AUTO_OFFSET_RESET = "autoOffsetReset";
    public static final String AUTO_OFFSET_RESET_DEFAULT = "earliest";

    public static final String AUTO_COMMIT_INTERVAL_MS = "autoCommitIntervalMs";
    public static final String AUTO_COMMIT_INTERVAL_MS_DEFAULT = "1000";

    public static final String SESSION_TIMEOUT_MS = "sessionTimeoutMs";
    public static final String SESSION_TIMEOUT_MS_DEFAULT = "30000";

    public static final String PARTITIONS = "partitions";
    public static final int PARTITIONS_DEFAULT = 1;

    public static final String POLL_SECONDS = "pollSeconds";
    public static final int POLL_SECONDS_DEFAULT = 5;
}
