package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("UUIDSerializerMetaData")
public class UUIDSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return UUIDSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        return null;
    }
}
