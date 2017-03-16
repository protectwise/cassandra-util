package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ayaz on 15/3/17.
 */
public class SerializerMetaDataFactory {
    private static final Logger logger = LoggerFactory.getLogger(SerializerMetaDataFactory.class);
    private static Map<String, SerializerMetaData> TYPE_SERIALIZER_TO_SERIALIZER_META_DATA_CLASS;
    static {
        Map<String, SerializerMetaData> aMap = new HashMap<>();

        aMap.put(SetSerializer.class.getName(), new SetSerializerMetaData());
        aMap.put(AsciiSerializer.class.getName(), new AsciiSerializerMetaData());
        aMap.put(UTF8Serializer.class.getName(), new UTF8SerializerMetaData());
        aMap.put(BooleanSerializer.class.getName(), new BooleanSerilizerMetaData());
        aMap.put(BytesSerializer.class.getName(), new BytesSerializerMetaData());
        aMap.put(DecimalSerializer.class.getName(), new DecimalSerializerMetaData());
        aMap.put(DoubleSerializer.class.getName(), new DoubleSerializerMetaData());
        aMap.put(EmptySerializer.class.getName(), new EmptySerializerMetaData());
        aMap.put(FloatSerializer.class.getName(), new FloatSerializerMetaData());
        aMap.put(InetAddressSerializer.class.getName(), new InetAddressSerializerMetaData());
        aMap.put(Int32Serializer.class.getName(), new Int32SerializerMetaData());
        aMap.put(IntegerSerializer.class.getName(), new IntegerSerializerMetaData());
        aMap.put(LongSerializer.class.getName(), new LongSerializerMetaData());
        aMap.put(TimestampSerializer.class.getName(), new TimestampSerializerMetaData());
        aMap.put(TimeUUIDSerializer.class.getName(), new TimeUUIDSerializerMetaData());
        aMap.put(UUIDSerializer.class.getName(), new UUIDSerializerMetaData());

        TYPE_SERIALIZER_TO_SERIALIZER_META_DATA_CLASS = Collections.unmodifiableMap(aMap);
    }
    public static SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(TYPE_SERIALIZER_TO_SERIALIZER_META_DATA_CLASS.containsKey(typeSerializer.getClass().getName())) {
            logger.debug("Got request for serializer meta data request for type serializer: {}", typeSerializer.getClass().getName());
            return TYPE_SERIALIZER_TO_SERIALIZER_META_DATA_CLASS.get(typeSerializer.getClass().getName()).getSerializerMetaData(typeSerializer);
        } else {
            throw new RuntimeException("Couldn't find serializer meta data for type serializer: " + typeSerializer.getClass().getName());
        }
    }
}
