package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("TimestampSerializerMetaData")
public class TimestampSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return TimestampSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        return null;
    }
}
