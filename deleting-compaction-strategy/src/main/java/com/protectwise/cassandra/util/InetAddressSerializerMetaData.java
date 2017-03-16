package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("InetAddressSerializerMetaData")
public class InetAddressSerializerMetaData extends SerializerMetaData{
    @Override
    public TypeSerializer getSerializer() {
        return InetAddressSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof InetAddressSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
