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
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.ColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractSimpleDeletingConvictor implements ISSTableScannerFilter, IOnDiskAtomFilter
{
	private static final Logger logger = LoggerFactory.getLogger(AbstractSimpleDeletingConvictor.class);
	public List<Cell> indexedColumnsInRow;
	protected ColumnFamilyStore cfs;
	protected Map<ByteBuffer, ColumnDefinition> columnDefs;

	/**
	 * @param cfs
	 * @param options
	 */
	public AbstractSimpleDeletingConvictor(ColumnFamilyStore cfs, Map<String, String> options)
	{
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

	protected Map<ByteBuffer, ColumnDefinition> getNamedColumnDefinitions()
	{
		if (columnDefs == null)
		{
			columnDefs = new HashMap<>(cfs.metadata.allColumns().size());
			for (ColumnDefinition cd : cfs.metadata.allColumns())
			{
				columnDefs.put(cd.name.bytes, cd);
			}
		}
		return columnDefs;
	}

	protected Map<ByteBuffer, ByteBuffer> getNamedPkColumns(OnDiskAtomIterator key)
	{
		return getNamedPkColumns(key.getKey().getKey());
	}

	protected Map<ByteBuffer, ByteBuffer> getNamedPkColumns(DecoratedKey key)
	{
		return getNamedPkColumns(key.getKey());
	}

	protected Map<ByteBuffer, ByteBuffer> getNamedPkColumns(ByteBuffer buf)
	{
		ByteBuffer[] keyParts = null;
		AbstractType<?> validator = cfs.metadata.getKeyValidator();
		if (validator instanceof CompositeType)
		{
			keyParts = ((CompositeType) validator).split(buf);
		}
		else
		{
			keyParts = new ByteBuffer[]{
					buf
			};
		}
		List<ColumnDefinition> pkc = cfs.metadata.partitionKeyColumns();
		Map<ByteBuffer, ByteBuffer> namedPkColumns = new HashMap<>(pkc.size());
		for (ColumnDefinition def : pkc)
		{
			namedPkColumns.put(def.name.bytes, keyParts[def.position()]);
		}
		return namedPkColumns;
	}

	@Override
	public boolean shouldKeepAtom(OnDiskAtomIterator partition, OnDiskAtom atom)
	{
		return true;
	}

	/**
	 * Allow a convictor to declare that it's in dry run mode.
	 * @return
	 */
	public boolean isDryRun()
	{
		return false;
	}
}
