package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("EmptySerializerMetaData")
public class EmptySerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return EmptySerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof EmptySerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
