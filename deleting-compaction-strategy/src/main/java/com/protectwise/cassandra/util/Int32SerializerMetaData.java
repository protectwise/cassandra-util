package com.protectwise.cassandra.util;

import com.protectwise.cassandra.db.compaction.BackupSinkForDeletingCompaction;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("Int32SerializerMetaData")
public class Int32SerializerMetaData extends SerializerMetaData {
    private static final Logger logger = LoggerFactory.getLogger(Int32SerializerMetaData.class);

    public Int32SerializerMetaData() {
        setQualifiedClassName(this.getClass().getName());
    }

    @Override
    public TypeSerializer getSerializer() {
        return Int32Serializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof Int32Serializer) {
            logger.debug("Inside serializermeta data: {}", this.getClass().getName());
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for int serializermetadata class");
        }
    }

}
