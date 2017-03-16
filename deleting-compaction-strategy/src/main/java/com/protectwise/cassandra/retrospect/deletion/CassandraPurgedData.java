package com.protectwise.cassandra.retrospect.deletion;

import com.protectwise.cassandra.util.SerializerMetaData;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ayaz on 9/3/17.
 */
public class CassandraPurgedData implements Serializable {

    private String ksName;

    private String cfName;

    private Set<String> partitonKeys = new HashSet<>();

    private Set<String> clusterKeys = new HashSet<>();

    private Map<String, SerializerMetaData> columnSerializerMetaDatas = new HashMap<>();

    private Map<String, byte[]> columnSerializedValues = new HashMap<>();

    public Set<String> getPartitonKeys() {
        return partitonKeys;
    }

    public void setPartitonKeys(Set<String> partitonKeys) {
        this.partitonKeys = partitonKeys;
    }

    public Set<String> getClusterKeys() {
        return clusterKeys;
    }

    public void setClusterKeys(Set<String> clusterKeys) {
        this.clusterKeys = clusterKeys;
    }

    public Map<String, SerializerMetaData> getColumnSerializerMetaDatas() {
        return columnSerializerMetaDatas;
    }

    public void setColumnSerializerMetaDatas(Map<String, SerializerMetaData> columnSerializerMetaDatas) {
        this.columnSerializerMetaDatas = columnSerializerMetaDatas;
    }

    public Map<String, byte[]> getColumnSerializedValues() {
        return columnSerializedValues;
    }

    public void setColumnSerializedValues(Map<String, byte[]> columnSerializedValues) {
        this.columnSerializedValues = columnSerializedValues;
    }


    public String getKsName() {
        return ksName;
    }

    public void setKsName(String ksName) {
        this.ksName = ksName;
    }


    public String getCfName() {
        return cfName;
    }

    public void setCfName(String cfName) {
        this.cfName = cfName;
    }

    public CassandraPurgedData addPartitonKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value) {
        partitonKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);
        columnSerializedValues.put(key, ByteBufferUtil.getArray(value));
        return this;
    }

    public CassandraPurgedData addClusteringKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value) {
        clusterKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);
        columnSerializedValues.put(key, ByteBufferUtil.getArray(value));
        return this;
    }

    public CassandraPurgedData addNonKeyColumn(String name, SerializerMetaData serializerMetaData, ByteBuffer value) {
        columnSerializerMetaDatas.put(name, serializerMetaData);
        columnSerializedValues.put(name, ByteBufferUtil.getArray(value));
        return this;
    }
}
