package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("UTF8SerializerMetaData")
public class UTF8SerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return UTF8Serializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        return null;
    }
}
