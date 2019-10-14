package com.provectus.kafka.connect;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonFieldPartitionerTest extends StorageSinkTestBase {
    private <T> JsonFieldPartitioner<T> getFieldPartitioner(String... fields) {
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
        config.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Arrays.asList(fields));
        config.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT);

        JsonFieldPartitioner<T> partitioner = new JsonFieldPartitioner<>();
        partitioner.configure(config);
        return partitioner;
    }

    public String generateEncodedPartitionFromMap(Map<String, Object> fieldMapping) {
        String delim = "/";
        return Utils.mkString(fieldMapping, "", "", "=", delim);
    }

    private static class TestRecord {
        private String uuid;
        private String ruuid;

        TestRecord(String uuid, String ruuid) {
            this.uuid = uuid;
            this.ruuid = ruuid;
        }

        public String getUuid() { return this.uuid; }
        public String getRuuid() { return this.ruuid; }
    }


    @Test
    public void testPartition() {
        String fieldName = "uuid";
        JsonFieldPartitioner<Object> partitioner = getFieldPartitioner(fieldName);

        TestRecord tr = new TestRecord("1234567890", "0987654321");

        SinkRecord sinkRecord = new SinkRecord("topic-name", 8, null, fieldName, null, tr, 0);
        String encodedPath = partitioner.encodePartition(sinkRecord);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put(fieldName, "1234567890");

        assertThat(encodedPath, is(generateEncodedPartitionFromMap(m)));
    }

}