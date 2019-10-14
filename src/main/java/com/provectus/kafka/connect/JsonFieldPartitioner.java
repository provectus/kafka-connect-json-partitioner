package com.provectus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class JsonFieldPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(JsonFieldPartitioner.class);
    private List<String> fieldNames;

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, Object> config) {
        this.fieldNames = (List)config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
        this.delim = (String)config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        return this.encodeJsonPartition(sinkRecord);
    }

    private String encodeJsonPartition(SinkRecord sinkRecord) {
        Object sinkRecordValue = sinkRecord.value();

        JsonNode decodedJsonSinkRecord = this.decodeJson(sinkRecordValue);

        StringBuilder builder = new StringBuilder();

        for(String fn : this.fieldNames) {
            if (builder.length() > 0) {
                builder.append(this.delim);
            }

            builder.append(fn);
            builder.append("=");
            builder.append(decodedJsonSinkRecord.get(fn).asText());
        }

        return builder.toString();
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                    Utils.join(fieldNames, ",")
            );
        }
        return partitionFields;
    }

    private JsonNode decodeJson(Object sinkRecordValue) {
        System.out.println(sinkRecordValue.getClass());
        try {
            return mapper.convertValue(sinkRecordValue, JsonNode.class);
        } catch (Exception e) {
            String excMsg = e.getMessage();
            log.error(excMsg);
            throw new PartitionException(excMsg);
        }
    }

}
