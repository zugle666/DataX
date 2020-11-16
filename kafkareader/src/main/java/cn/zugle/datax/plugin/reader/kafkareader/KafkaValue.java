package cn.zugle.datax.plugin.reader.kafkareader;

/**
 * @author zhuzhq 2020-11-13
 */
public class KafkaValue {

    private String key;

    /**
     * @see com.alibaba.datax.common.element.Column.Type
     */
    private String type;

    private Object value;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
