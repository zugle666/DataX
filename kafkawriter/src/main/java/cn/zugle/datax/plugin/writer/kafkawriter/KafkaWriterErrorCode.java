package cn.zugle.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaWriterErrorCode implements ErrorCode {

    ILLEGAL_COLUMN_VALUE("ILLEGAL_COLUMN_VALUE","参数column错误"),
    ILLEGAL_KEYS_VALUE("ILLEGAL_KEYS_VALUE","参数keys错误");;

    private final String code;

    private final String description;

    KafkaWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
