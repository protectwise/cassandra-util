package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("IntegerSerializerMetaData")
public class IntegerSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return IntegerSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof IntegerSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
