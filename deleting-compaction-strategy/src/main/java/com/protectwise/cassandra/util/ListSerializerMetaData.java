package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 16/3/17.
 */
@JsonTypeName("ListSerializerMetaData")
public class ListSerializerMetaData extends SerializerMetaData {
    private SerializerMetaData elementSerializerMetaData;


    public SerializerMetaData getElementSerializerMetaData() {
        return elementSerializerMetaData;
    }

    public void setElementSerializerMetaData(SerializerMetaData elementSerializerMetaData) {
        this.elementSerializerMetaData = elementSerializerMetaData;
    }

    @Override
    public TypeSerializer getSerializer() {
        return ListSerializer.getInstance(elementSerializerMetaData.getSerializer());
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof ListSerializer) {
            ListSerializerMetaData serializerMetaData = new ListSerializerMetaData();
            serializerMetaData.setElementSerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((ListSerializer) typeSerializer).elements));
            return serializerMetaData;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}
