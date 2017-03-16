package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 16/3/17.
 */
@JsonTypeName("MapSerializerMetaData")
public class MapSerializerMetaData extends SerializerMetaData {
    private SerializerMetaData keySerializerMetaData;
    private SerializerMetaData valueSerializerMetaData;

    public SerializerMetaData getValueSerializerMetaData() {
        return valueSerializerMetaData;
    }

    public void setValueSerializerMetaData(SerializerMetaData valueSerializerMetaData) {
        this.valueSerializerMetaData = valueSerializerMetaData;
    }

    public SerializerMetaData getKeySerializerMetaData() {
        return keySerializerMetaData;
    }

    public void setKeySerializerMetaData(SerializerMetaData keySerializerMetaData) {
        this.keySerializerMetaData = keySerializerMetaData;
    }

    @Override
    public TypeSerializer getSerializer() {
        return MapSerializer.getInstance(keySerializerMetaData.getSerializer(), valueSerializerMetaData.getSerializer());
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof MapSerializer) {
            MapSerializerMetaData serializerMetaData = new MapSerializerMetaData();
            serializerMetaData.setKeySerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((MapSerializer) typeSerializer).keys));
            serializerMetaData.setValueSerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((MapSerializer) typeSerializer).values));
            return serializerMetaData;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
