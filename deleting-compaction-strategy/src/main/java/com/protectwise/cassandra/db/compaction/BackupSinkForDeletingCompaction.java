/*
 * Copyright 2016 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.db.compaction;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupSinkForDeletingCompaction implements IDeletedRecordsSink
{
	private static final Logger logger = LoggerFactory.getLogger(BackupSinkForDeletingCompaction.class);

	protected final ColumnFamilyStore cfs;
	protected final ColumnFamily columnFamily;
	protected final File targetDirectory;
	protected final long keysPerSSTable;

	protected SSTableWriter writer;
	protected DecoratedKey currentKey;
	protected boolean isEmpty = true;

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory)
	{
		// TODO: Wow, this key estimate is probably grossly over-estimated...  Not sure how to get a better one here.
		this(cfs, targetDirectory, cfs.estimateKeys() / cfs.getLiveSSTableCount());
	}

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory, long keyEstimate)
	{
		this.cfs = cfs;
		this.targetDirectory = targetDirectory;
		this.keysPerSSTable = keyEstimate;

		// Right now we're just doing one sink per compacted sstable, so they'll be pre-sorted, meaning
		// we don't need to bother resorting the data.
		columnFamily = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.getColumnFamilyName());
	}

	protected void flush()
	{
		if (!columnFamily.isEmpty())
		{
			writer.append(currentKey, columnFamily);
			columnFamily.clear();
		}
	}

	@Override
	public void accept(OnDiskAtomIterator partition)
	{
		flush();
		currentKey = partition.getKey();
		// Write through the entire partition.
		while (partition.hasNext())
		{
			accept(partition.getKey(), partition.next());
		}
	}

	@Override
	public void accept(DecoratedKey key, OnDiskAtom cell)
	{
		if (currentKey != key)
		{
			flush();
			currentKey = key;
		}

		columnFamily.addAtom(cell);
		isEmpty = false;
	}

	@Override
	public void begin()
	{
		writer = new SSTableWriter(
				cfs.getTempSSTablePath(targetDirectory),
				keysPerSSTable,
				ActiveRepairService.UNREPAIRED_SSTABLE,
				cfs.metadata,
				cfs.partitioner,
				new MetadataCollector(cfs.metadata.comparator)
		);
		logger.info("Opening backup writer for {}", writer.getFilename());
	}

	@Override
	public void close() throws IOException
	{
		if (!isEmpty)
		{
			flush();
			logger.info("Cleanly closing backup operation for {}", writer.getFilename());
			writer.close();
		}
		else
		{
			// If deletion convicted nothing, then don't bother writing an empty backup file.
			abort();
		}
	}

	/**
	 * Abort the operation and discard any outstanding data.
	 * Only one of close() or abort() should be called.
	 */
	@Override
	public void abort()
	{
		logger.info("Aborting backup operation for {}", writer.getFilename());
		columnFamily.clear();
		writer.abort();
	}
}
