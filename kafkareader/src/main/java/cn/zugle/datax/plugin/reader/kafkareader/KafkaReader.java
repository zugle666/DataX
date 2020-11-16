package cn.zugle.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @author zhuzhq 2020-11-13
 */
public class KafkaReader extends Reader {

    private static final Logger logger = LoggerFactory.getLogger(KafkaReader.class);

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;
        private KafkaConsumer<String, String> kafkaConsumer;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.kafkaConsumer = buildConsumer(originalConfig);
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            Integer partitions = this.originalConfig.getInt(KeyConstant.PARTITIONS, KeyConstant.PARTITIONS_DEFAULT);
            for (int i = 0; i < partitions; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }

    }

    public static class Task extends Reader.Task {
        private Configuration kafkaSliceConfig;
        private KafkaConsumer<String, String> kafkaConsumer;
        private Boolean enableAutoCommit;
        private String topic;

        @Override
        public void init() {
            this.kafkaSliceConfig = super.getPluginJobConf();
            this.topic = kafkaSliceConfig.getString(KeyConstant.TOPIC, KeyConstant.TOPIC_DEFAULT);
            this.enableAutoCommit = Boolean.valueOf(kafkaSliceConfig.getString(KeyConstant.ENABLE_AUTO_COMMIT,
                    KeyConstant.ENABLE_AUTO_COMMIT_DEFAULT));
        }

        @Override
        public void startRead(RecordSender recordSender) {
            this.kafkaConsumer = buildConsumer(kafkaSliceConfig);
            this.kafkaConsumer.subscribe(Collections.singletonList(topic));
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMinutes(1));
                for (ConsumerRecord<String, String> record : records) {
                    Record r = recordSender.createRecord();
                    JSONArray jsonArray = JSONArray.parseArray(record.value());
                    for (int i=0; i<jsonArray.size();i++) {
                        KafkaValue kafkaValue = jsonArray.getObject(i, KafkaValue.class);
                        Column.Type type = Column.Type.valueOf(kafkaValue.getType());
                        switch (type) {
                            case STRING:
                                r.addColumn(new StringColumn(String.valueOf(kafkaValue.getValue())));
                                break;
                            case DOUBLE:
                                r.addColumn(new DoubleColumn(String.valueOf(kafkaValue.getValue())));
                                break;
                            case BYTES:
                                r.addColumn(new BytesColumn((byte[])kafkaValue.getValue()));
                                break;
                            case INT:
                            case LONG:
                                r.addColumn(new LongColumn(String.valueOf(kafkaValue.getValue())));
                                break;
                            case DATE:
                                r.addColumn(new DateColumn(Long.valueOf(String.valueOf(kafkaValue.getValue()))));
                                break;
                            case BOOL:
                                r.addColumn(new BoolColumn(Boolean.valueOf(String.valueOf(kafkaValue.getValue()))));
                                break;
                            case BAD:
                            case NULL:
                                r.addColumn(null);
                                break;
                        }
                    }
                    recordSender.sendToWriter(r);
                }
                if (!enableAutoCommit) {
                    kafkaConsumer.commitSync();
                }
        }

        @Override
        public void destroy() {

        }
    }

    private static KafkaConsumer<String, String> buildConsumer(Configuration originalConfig) {
        Properties props = new Properties();
        String bootstrapServers = originalConfig.getString(KeyConstant.BOOTSTRAP_SERVERS,KeyConstant.BOOTSTRAP_SERVERS_DEFAULT);
        String groupId = originalConfig.getString(KeyConstant.GROUP_ID,KeyConstant.GROUP_ID_DEFAULT);
        String enableAutoCommit = originalConfig.getString(KeyConstant.ENABLE_AUTO_COMMIT, KeyConstant.ENABLE_AUTO_COMMIT_DEFAULT);
        String keyDeserializer = originalConfig.getString(KeyConstant.KEY_DESERIALIZER,KeyConstant.KEY_DESERIALIZER_DEFAULT);
        String valueDeserializer = originalConfig.getString(KeyConstant.VALUE_DESERIALIZER,KeyConstant.VALUE_DESERIALIZER_DEFAULT);
        String autoOffsetReset = originalConfig.getString(KeyConstant.AUTO_OFFSET_RESET,KeyConstant.AUTO_OFFSET_RESET_DEFAULT);
        String autoCommitIntervalMs = originalConfig.getString(KeyConstant.AUTO_COMMIT_INTERVAL_MS,KeyConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT);
        String sessionTimeoutMs = originalConfig.getString(KeyConstant.SESSION_TIMEOUT_MS,KeyConstant.SESSION_TIMEOUT_MS_DEFAULT);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", valueDeserializer);
        props.put("enable.auto.commit", Boolean.valueOf(enableAutoCommit));
        props.put("auto.offset.reset", autoOffsetReset);
        props.put("auto.commit.interval.ms", Integer.valueOf(autoCommitIntervalMs));
        props.put("session.timeout.ms", sessionTimeoutMs);
        return new KafkaConsumer<>(props);
    }

}
