package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="https://github.com/harveyyue">Harvey Yue</a>
 */
public class TableMapEventMetadataDeserializerTest {

    /**
     * https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/include/rows_event.h#L185
     * There are some optional metadata defined. They are listed in the table
     * Table_table_map_event_optional_metadata. Optional metadata fields
     * follow null_bits. Whether binlogging an optional metadata is decided by the
     * server. The order is not defined, so they can be binlogged in any order.
     *
     * @throws IOException
     */
    @Test
    public void deserialize() throws IOException {
        byte[] metadataIncludingUnknownFieldType = {1, 2, 0, -128, 2, 9, 83, 6, 63, 7, 63, 8, 63, 9, 63};
        TableMapEventMetadataDeserializer deserializer = new TableMapEventMetadataDeserializer();
        TableMapEventMetadata tableMapEventMetadata =
            deserializer.deserialize(new ByteArrayInputStream(metadataIncludingUnknownFieldType), 23, 8, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7)); // suppose there numeric Columns Idx are 0, 1, 2, 3, 4, 5, 6, 7

        Map<Integer, Integer> expectedCharsetCollations = new LinkedHashMap<>();
        expectedCharsetCollations.put(6, 63);
        expectedCharsetCollations.put(7, 63);
        expectedCharsetCollations.put(8, 63);
        expectedCharsetCollations.put(9, 63);

        assertEquals(tableMapEventMetadata.getDefaultCharset().getDefaultCharsetCollation(), 83);
        assertEquals(tableMapEventMetadata.getDefaultCharset().getCharsetCollations(), expectedCharsetCollations);
    }
}
