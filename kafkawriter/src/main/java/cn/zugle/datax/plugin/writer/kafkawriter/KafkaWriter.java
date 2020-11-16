package cn.zugle.datax.plugin.writer.kafkawriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * @author zhuzhq 2020-11-13
 */
public class KafkaWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private KafkaProducer<String, String> kafkaProducer;
        private int partitions;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < partitions; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.kafkaProducer = buildProducer(this.originalConfig);
            partitions = originalConfig.getInt(KeyConstant.PARTITIONS, KeyConstant.PARTITIONS_DEFAULT);
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Writer.Task {
        private Configuration kafkaSliceConfig;
        private KafkaProducer<String, String> kafkaProducer;
        private List<String> columns = null;
        private List<String> keys = null;
        private int columnNumber = 0;
        private String topic;

        @Override
        public void init() {
            this.kafkaSliceConfig = super.getPluginJobConf();
            this.kafkaProducer = buildProducer(this.kafkaSliceConfig);
            this.columns = kafkaSliceConfig.getList(KeyConstant.COLUMN, new ArrayList<>(), String.class);
            this.columnNumber = this.columns.size();
            topic = kafkaSliceConfig.getString(KeyConstant.TOPIC, KeyConstant.TOPIC_DEFAULT);
            this.keys = kafkaSliceConfig.getList(KeyConstant.KEYS, new ArrayList<>(), String.class);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != this.columnNumber) {
                    // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                    throw DataXException
                            .asDataXException(
                                    KafkaWriterErrorCode.ILLEGAL_COLUMN_VALUE,
                                    String.format(
                                            "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                            record.getColumnNumber(),
                                            this.columnNumber));
                }
                List<KafkaValue> values = new ArrayList<>();
                Map<String, Object> keyMap = new HashMap<>();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    KafkaValue value = new KafkaValue();
                    value.setType(column.getType().name());
                    value.setKey(this.columns.get(i));
                    switch (column.getType()) {
                        case INT:
                            value.setValue(column.asBigInteger());
                            break;
                        case BOOL:
                            value.setValue(column.asBoolean());
                            break;
                        case DATE:
                            value.setValue(column.asDate().getTime());
                            break;
                        case LONG:
                            value.setValue(column.asLong());
                            break;
                        case BYTES:
                            value.setValue(column.asBytes());
                            break;
                        case DOUBLE:
                            value.setValue(column.asDouble());
                            break;
                        case STRING:
                            value.setValue(column.asString());
                            break;
                        case BAD:
                        case NULL:
                            value.setValue(null);
                            break;
                    }
                    values.add(value);
                    if (keys.contains(value.getKey())) {
                        keyMap.put(value.getKey(), value.getValue());
                    }
                }
                String content = JSONObject.toJSONString(values);
                String key = JSONObject.toJSONString(keyMap);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, content);
                kafkaProducer.send(producerRecord);
            }
        }

    }

    private static KafkaProducer<String, String> buildProducer(Configuration originalConfig) {
        Properties props = new Properties();
        String bootstrapServers = originalConfig.getString(KeyConstant.BOOTSTRAP_SERVERS, KeyConstant.BOOTSTRAP_SERVERS_DEFAULT);
        String acks = originalConfig.getString(KeyConstant.ACKS, KeyConstant.ACKS_DEFAULT);
        String retries = originalConfig.getString(KeyConstant.RETRIES, KeyConstant.RETRIES_DEFAULT);
        String batchSize = originalConfig.getString(KeyConstant.BATCH_SIZE, KeyConstant.BATCH_SIZE_DEFAULT);
        String lingerMs = originalConfig.getString(KeyConstant.LINGER_MS, KeyConstant.LINGER_MS_DEFAULT);
        String bufferMemory = originalConfig.getString(KeyConstant.BUFFER_MEMORY, KeyConstant.BUFFER_MEMORY_DEFAULT);
        String keySerializer = originalConfig.getString(KeyConstant.KEY_DESERIALIZER, KeyConstant.KEY_DESERIALIZER_DEFAULT);
        String valueSerializer = originalConfig.getString(KeyConstant.VALUE_DESERIALIZER, KeyConstant.VALUE_DESERIALIZER_DEFAULT);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("batch.size", Integer.valueOf(batchSize));
        props.put("linger.ms", Integer.valueOf(lingerMs));
        props.put("buffer.memory", Integer.valueOf(bufferMemory));
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        return new KafkaProducer<>(props);
    }
}
