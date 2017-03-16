package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("DecimalSerializerMetaData")
public class DecimalSerializerMetaData extends SerializerMetaData{
    @Override
    public TypeSerializer getSerializer() {
        return DecimalSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof DecimalSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
