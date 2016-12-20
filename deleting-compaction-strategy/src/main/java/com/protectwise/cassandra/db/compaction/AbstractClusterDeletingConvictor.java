/*
 * Copyright 2016 ProtectWise, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.protectwise.cassandra.db.compaction;

import com.protectwise.cassandra.db.columniterator.IOnDiskAtomFilter;
import com.protectwise.cassandra.io.sstable.ISSTableScannerFilter;
import com.protectwise.cassandra.util.PrintHelper;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.AbstractCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * AbstractSimpleDeletingConvictor can convict either entire partitions or individual atoms (cells).
 */
public abstract class AbstractClusterDeletingConvictor extends AbstractSimpleDeletingConvictor
{

	public static class ColumnNotFoundException extends RuntimeException
	{
		public ColumnNotFoundException(String message)
		{
			super(message);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(AbstractClusterDeletingConvictor.class);

	protected final CType clusterType;
	protected final List<ColumnDefinition> clusterKeys;

	protected Composite testedCk = null;
	protected Token testedToken;
	protected boolean passedCk = false;
	protected Map<ByteBuffer, ColumnDefinition> namedColumnDefs;

	/**
	 * @param cfs
	 * @param options
	 */
	public AbstractClusterDeletingConvictor(ColumnFamilyStore cfs, Map<String, String> options)
	{
		super(cfs, options);

		clusterKeys = cfs.metadata.clusteringColumns();

		if (clusterKeys.size() == 1)
		{
			clusterType = new SimpleCType(clusterKeys.get(0).type);
		}
		else
		{
			List<AbstractType<?>> types = new ArrayList<>(clusterKeys.size());
			for (ColumnDefinition def : clusterKeys)
			{
				types.add(def.type);
			}
			clusterType = new CompoundCType(types);
		}

		this.cfs = cfs;
	}

	/**
	 * Called by {{{DeletingCompactionStrategy.validateOptions}}} to allow the convictor to
	 * read and validate convictor-specific options at the same time.
	 * <p/>
	 * See {@link org.apache.cassandra.db.compaction.AbstractCompactionStrategy#validateOptions(Map)}
	 *
	 * @param options
	 * @return
	 */
	public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
	{
		return options;
	}

	public Map<ByteBuffer, ByteBuffer> getClusteringValues(Composite name)
	{
		Map<ByteBuffer, ByteBuffer> namesAndValues = new HashMap<>();
		for (ColumnDefinition def : cfs.metadata.clusteringColumns())
		{
			namesAndValues.put(def.name.bytes, getBytes(name, def));
		}
		return namesAndValues;
	}

	@Override
	public boolean shouldKeepAtom(OnDiskAtomIterator partition, OnDiskAtom atom)
	{
		if (testedToken != null && testedCk != null && partition.getKey().getToken() == testedToken && testedCk.isPrefixOf(clusterType, atom.name()))
		{
			// We have already tested this cluster key, and this atom is in the same cluster key.
			// Conviction for this atom is the same as conviction for the prior atom.
		}
		else
		{
			// This atom is a new cluster key, test it for conviction.
			testedToken = partition.getKey().getToken();
			testedCk = buildClusterKey(atom.name());
			passedCk = shouldKeepCluster(partition, testedCk);
		}

		return passedCk;
	}

	abstract public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name);

	Composite buildClusterKey(Composite name)
	{
		final CBuilder builder = clusterType.builder();
		for (int i = 0; builder.remainingCount() > 0; i++)
		{
			builder.add(name.get(i));
		}
		return builder.build();
	}


	ColumnDefinition getColumn(String name)
	{
		try
		{
			return getColumn(ByteBuffer.wrap(name.getBytes("UTF-8")));
		}
		catch (UnsupportedEncodingException e)
		{
			logger.error("UTF-8 encoding is not supported on this system.");
		}
		catch (ColumnNotFoundException e)
		{
			throw new ColumnNotFoundException("The column " + name + " cannot be located as part of the clustering key.  Clustering conviction requires considering only the clustering, other columns may not be available for reliable reads.");
		}
		return null;
	}

	ColumnDefinition getColumn(ByteBuffer name)
	{
		if (namedColumnDefs == null)
		{
			namedColumnDefs = new HashMap<>();
			for (ColumnDefinition def : clusterKeys)
			{
				namedColumnDefs.put(def.name.bytes, def);
			}
		}

		if (namedColumnDefs.containsKey(name))
		{
			return namedColumnDefs.get(name);
		}
		else
		{
			throw new ColumnNotFoundException("Column Not Found in Clustering Key");
		}
	}

	ByteBuffer getComponent(Composite ck, String name)
	{
		return ck.get(getColumn(name).position());
	}

	public Long getLong(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Long.class : "Column '" + column.name + "' is not of type Long, but of type " + ser.getType().getSimpleName();
		return (Long)ser.deserialize(ck.get(column.position()));
	}

	public Long getLong(Composite ck, String name)
	{
		return getLong(ck, getColumn(name));
	}

	public Integer getInteger(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Integer.class : "Column '" + column.name + "' is not of type Integer, but of type " + ser.getType().getSimpleName();
		return (Integer)ser.deserialize(ck.get(column.position()));
	}

	public Integer getInteger(Composite ck, String name)
	{
		return getInteger(ck, getColumn(name));
	}

	public String getString(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == String.class : "Column '" + column.name + "' is not of type String, but of type " + ser.getType().getSimpleName();
		return (String)ser.deserialize(ck.get(column.position()));
	}

	public String getString(Composite ck, String name)
	{
		return getString(ck, getColumn(name));
	}

	public ByteBuffer getBytes(Composite ck, ColumnDefinition column)
	{
		return ck.get(column.position());
	}

	public ByteBuffer getBytes(Composite ck, ByteBuffer name)
	{
		return getBytes(ck, getColumn(name));
	}

	public Date getDate(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Date.class : "Column '" + column.name + "' is not of type Date, but of type " + ser.getType().getSimpleName();
		return (Date)ser.deserialize(ck.get(column.position()));
	}

	public Date getDate(Composite ck, String name)
	{
		return getDate(ck, getColumn(name));
	}

	public UUID getUUID(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == UUID.class : "Column '" + column.name + "' is not of type UUID, but of type " + ser.getType().getSimpleName();
		return (UUID)ser.deserialize(ck.get(column.position()));
	}

	public UUID getUUID(Composite ck, String name)
	{
		return getUUID(ck, getColumn(name));
	}

	public Double getDouble(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Double.class : "Column '" + column.name + "' is not of type Double, but of type " + ser.getType().getSimpleName();
		return (Double)ser.deserialize(ck.get(column.position()));
	}

	public Double getDouble(Composite ck, String name)
	{
		return getDouble(ck, getColumn(name));
	}

	public Boolean getBoolean(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Boolean.class : "Column '" + column.name + "' is not of type Boolean, but of type " + ser.getType().getSimpleName();
		return (Boolean)ser.deserialize(ck.get(column.position()));
	}

	public Boolean getBoolean(Composite ck, String name)
	{
		return getBoolean(ck, getColumn(name));
	}

	public BigDecimal getBigDecimal(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == BigDecimal.class : "Column '" + column.name + "' is not of type BigDecimal, but of type " + ser.getType().getSimpleName();
		return (BigDecimal)ser.deserialize(ck.get(column.position()));
	}

	public BigDecimal getBigDecimal(Composite ck, String name)
	{
		return getBigDecimal(ck, getColumn(name));
	}

	public Float getFloat(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == Float.class : "Column '" + column.name + "' is not of type Float, but of type " + ser.getType().getSimpleName();
		return (Float)ser.deserialize(ck.get(column.position()));
	}

	public Float getFloat(Composite ck, String name)
	{
		return getFloat(ck, getColumn(name));
	}

	public InetAddress getInetAddress(Composite ck, ColumnDefinition column)
	{
		TypeSerializer<?> ser = column.type.getSerializer();
		assert ser.getType() == InetAddress.class : "Column '" + column.name + "' is not of type InetAddress, but of type " + ser.getType().getSimpleName();
		return (InetAddress)ser.deserialize(ck.get(column.position()));
	}

	public InetAddress getInetAddress(Composite ck, String name)
	{
		return getInetAddress(ck, getColumn(name));
	}

}
