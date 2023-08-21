package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
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
        byte[] metadataIncludingUnknownFieldType = { 1, 1, 29, 2, 9, 83, 6, 63, 7, 63, 8, 63, 9, 63 };
        TableMapEventMetadataDeserializer deserializer = new TableMapEventMetadataDeserializer();
        // suppose there Columns idx likes
        // col 0, 1, 10, 11,,, non numeric
        // col 2, 3, 4, 8 signed
        // col 5, 6, 7, 9 unsigned
        List<Integer> numericIndexWithAllColumn = Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9);
        TableMapEventMetadata tableMapEventMetadata =
            deserializer.deserialize(new ByteArrayInputStream(metadataIncludingUnknownFieldType), 23, 8,
                                     numericIndexWithAllColumn);

        Map<Integer, Integer> expectedCharsetCollations = new LinkedHashMap<>();
        expectedCharsetCollations.put(6, 63);
        expectedCharsetCollations.put(7, 63);
        expectedCharsetCollations.put(8, 63);
        expectedCharsetCollations.put(9, 63);

        // metadataIncludingUnknownFieldType[2] = 29
        // bin 00011101
        // 2, 3, 4, 5(hit), 6(hit), 7(hit), 8, 9(hit
        BitSet expectAllColumnBitSet = new BitSet();
        expectAllColumnBitSet.set(5);
        expectAllColumnBitSet.set(6);
        expectAllColumnBitSet.set(7);
        expectAllColumnBitSet.set(9);

        assertEquals(tableMapEventMetadata.getDefaultCharset().getDefaultCharsetCollation(), 83);
        assertEquals(tableMapEventMetadata.getDefaultCharset().getCharsetCollations(),
                     expectedCharsetCollations);
        assertEquals(tableMapEventMetadata.getSignedness(), expectAllColumnBitSet);
    }
}
