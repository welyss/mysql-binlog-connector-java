package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.*;

public class MysqlGtidEventDataDeserializerTest {

    private GtidEventDataDeserializer deserializer = new GtidEventDataDeserializer();

    @Test
    public void testDeserialize() throws IOException {
        GtidEventData data = deserializer.deserialize(new ByteArrayInputStream(
            new byte[]{
                0x03, //flags
                (byte) 0xe6, 0x11, 0x16, 0x2c, 0x50, 0x78, (byte) 0xbc, 0x24, // sourceId leastSignificantBits little endian
                0x02, 0x00, 0x11, (byte) 0xac, 0x42, 0x02, 0x73, (byte) 0xa0, //sourceId mostSignificantBits little endian
                (byte) 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 // sequence little endian
            }
        ));
        assertEquals(data.getFlags(), 0x03);
        assertEquals(data.getMySqlGtid().toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
    }
}
