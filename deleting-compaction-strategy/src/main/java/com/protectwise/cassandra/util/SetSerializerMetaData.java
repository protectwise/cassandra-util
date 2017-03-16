package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("SetSerializerMetaData")
public class SetSerializerMetaData extends SerializerMetaData {
    private SerializerMetaData elementSerializerMetaData;


    public SerializerMetaData getElementSerializerMetaData() {
        return elementSerializerMetaData;
    }

    public void setElementSerializerMetaData(SerializerMetaData elementSerializerMetaData) {
        this.elementSerializerMetaData = elementSerializerMetaData;
    }

    @Override
    public TypeSerializer getSerializer() {
        return SetSerializer.getInstance(elementSerializerMetaData.getSerializer());
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
       if(typeSerializer instanceof  SetSerializer) {
           SetSerializerMetaData serializerMetaData = new SetSerializerMetaData();
           serializerMetaData.setElementSerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((SetSerializer) typeSerializer).elements));
           return serializerMetaData;
       } else {
           throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
       }
    }
}
