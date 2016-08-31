/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.io.sstable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.RateLimiter;
import com.protectwise.cassandra.db.columniterator.FilteringOnDiskAtomIterator;
import com.protectwise.cassandra.db.columniterator.IOnDiskAtomFilter;
import com.protectwise.cassandra.db.compaction.IDeletedRecordsSink;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FilteringSSTableScanner implements ISSTableScanner
{
	private static final Logger logger = LoggerFactory.getLogger(FilteringSSTableScanner.class);

	protected final ISSTableScanner underlying;
	protected final ISSTableScannerFilter filter;
	protected final Iterator<OnDiskAtomIterator> iterator;
	protected final IOnDiskAtomFilter atomFilter;
	protected final ColumnFamilyStore cfs;
	protected final boolean dryRun;
	protected final IDeletedRecordsSink backupSink;

	protected FilteringOnDiskAtomIterator prev;
	protected boolean isCompleted = false;

	public long keptPartitionKeys = 0;
	public long droppedPartitionKeys = 0;

	public long keptClusterKeys = 0;
	public long droppedClusterKeys = 0;

	protected final long jobStartTime = System.currentTimeMillis();
	protected long lastStatusReport;
	protected long lastKeptPartitionKeys = 0;
	protected long lastDroppedPartitionKeys = 0;
	protected long lastKeptClusterKeys = 0;
	protected long lastDroppedClusterKeys = 0;

	public FilteringSSTableScanner(
			ISSTableScanner underlying,
			final ISSTableScannerFilter filter,
			IOnDiskAtomFilter atomFilter,
			final ColumnFamilyStore cfs,
			final boolean dryRun,
			final IDeletedRecordsSink backupSink,
			final long statusReportInterval
	)
	{
		this.underlying = underlying;
		this.filter = filter;
		this.atomFilter = atomFilter;
		this.cfs = cfs;
		this.dryRun = dryRun;
		this.backupSink = backupSink;
		lastStatusReport = System.currentTimeMillis();

		if (backupSink != null && !dryRun)
		{
			backupSink.begin();
		}

		iterator = Iterators.filter(underlying, new Predicate<OnDiskAtomIterator>()
		{
			@Override
			public boolean apply(OnDiskAtomIterator input)
			{
				long now = System.currentTimeMillis();
				if (statusReportInterval > 0l && now - lastStatusReport > statusReportInterval)
				{
					statusReport();
				}

				if (filter.shouldKeepPartition(input))
				{
					keptPartitionKeys += 1;
					return true;
				}
				else if (dryRun)
				{
					droppedPartitionKeys += 1;
					return true;
				}
				else
				{
					droppedPartitionKeys += 1;
					deleteFromIndexesAndFillBackupSink(input);
					cfs.invalidateCachedRow(input.getKey());
					return false;
				}
			}
		});
	}

	/**
	 * Deletes a record whose partition key was convicted from indexes, while
	 * also filling the backup sink if necessary.  These two operations both
	 * rely on iterating the input, so they must be done in tandem for
	 * performance (avoiding double iterating, and avoiding rewinding
	 * on a sequential read).
	 * @param input
	 */
	protected void deleteFromIndexesAndFillBackupSink(OnDiskAtomIterator input)
	{
		// Clean up all indexes for this partition
		if (cfs.indexManager.hasIndexes())
		{
			final List<Cell> indexedColumnsInRow = new ArrayList<>();

			// If there are indexes, then the backup sink operation will exhaust the iterator, we need to
			// manage indexes at the same time as backup sink is consuming it.  We'll use a simple noop
			// filter to peek at each atom as the backup sink writes it.
			if (backupSink != null)
			{
				backupSink.accept(new FilteringOnDiskAtomIterator(input, new IOnDiskAtomFilter()
				{
					@Override
					public boolean shouldKeepAtom(OnDiskAtomIterator partition, OnDiskAtom column)
					{
						if (column instanceof Cell && cfs.indexManager.indexes((Cell) column))
						{
							indexedColumnsInRow.add((Cell) column);
						}
						// Keep all records so they make it into the backup sink.
						return true;
					}
				}, cfs, false, null));
			}
			else
			{
				// With no backup sink, we just iterate this ourselves.
				while (input.hasNext())
				{
					OnDiskAtom column = input.next();
					if (column instanceof Cell && cfs.indexManager.indexes((Cell) column))
					{
						indexedColumnsInRow.add((Cell) column);
					}
				}
			}

			if (!indexedColumnsInRow.isEmpty())
			{
				// acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
				try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start())
				{
					cfs.indexManager.deleteFromIndexes(input.getKey(), indexedColumnsInRow, opGroup);
				}
			}

		}
		else
		{
			// If there are no indexes, we can hand this off to the backup sink directly with no interference.
			if (backupSink != null)
			{
				backupSink.accept(input);
			}
		}

	}

	public static ISSTableScanner getScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
	{
		return SSTableScanner.getScanner(sstable, dataRange, limiter);
	}

	public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
	{
		return SSTableScanner.getScanner(sstable, tokenRanges, limiter);
	}

	@Override
	public void close() throws IOException
	{
		if (backupSink != null)
		{
			// If we reach the end of the iterator, then we've completed this sstable.
			// Unfortunately this may lead to completed backup files for sstables involved
			// in aborted compactions, because one or more sstables may complete before
			// the compaction does, but we only get notified on completion of an sstable.
			if (isCompleted)
			{
				backupSink.close();
			}
			else
			{
				backupSink.abort();
			}
		}
		underlying.close();
	}

	@Override
	public long getLengthInBytes()
	{
		return underlying.getLengthInBytes();
	}

	@Override
	public long getCurrentPosition()
	{
		return underlying.getCurrentPosition();
	}

	@Override
	public String getBackingFiles()
	{
		return underlying.getBackingFiles();
	}

	@Override
	public String toString()
	{
		return underlying.toString();
	}

	/**
	 * Returns {@code true} if the iteration has more elements.
	 * (In other words, returns {@code true} if {@link #next} would
	 * return an element rather than throwing an exception.)
	 *
	 * @return {@code true} if the iteration has more elements
	 */
	@Override
	public boolean hasNext()
	{
		if (prev != null)
		{
			keptClusterKeys += prev.kept;
			droppedClusterKeys += prev.filtered;
			if (!dryRun)
			{
				if (prev.filtered > 0)
				{
					cfs.invalidateCachedRow(prev.getKey());
				}
				if (prev.kept == 0)
				{
					// TODO: Should we clean up the partition key from indexes here?
				}
			}

			prev = null;
		}
		if (iterator.hasNext())
		{
			return true;
		}
		else
		{
			// Log a status report, this compaction has completed.
			statusReport();
			if (dryRun)
			{
				logger.info(String.format("DRY RUN - %s.%s Partitions [Skipped,Kept]: [%d,%d]; Clusters [Skipped,Kept]: [%d,%d].  This was a dry run only, no data was actually deleted.", cfs.keyspace.getName(), cfs.getColumnFamilyName(), droppedPartitionKeys, keptPartitionKeys, droppedClusterKeys, keptClusterKeys));
			}
			else
			{
				logger.info(String.format("%s.%s Partitions [Skipped,Kept]: [%d,%d]; Clusters [Skipped,Kept]: [%d,%d]", cfs.keyspace.getName(), cfs.getColumnFamilyName(), droppedPartitionKeys, keptPartitionKeys, droppedClusterKeys, keptClusterKeys));
			}
			isCompleted = true;
			return false;
		}
	}

	/**
	 * Returns the next element in the iteration.
	 *
	 * @return the next element in the iteration
	 * @throws NoSuchElementException if the iteration has no more elements
	 */
	@Override
	public OnDiskAtomIterator next()
	{
		prev = new FilteringOnDiskAtomIterator(iterator.next(), atomFilter, cfs, dryRun, backupSink);
		return prev;
	}

	/**
	 * Removes from the underlying collection the last element returned
	 * by this iterator (optional operation).  This method can be called
	 * only once per call to {@link #next}.  The behavior of an iterator
	 * is unspecified if the underlying collection is modified while the
	 * iteration is in progress in any way other than by calling this
	 * method.
	 *
	 * @throws UnsupportedOperationException if the {@code remove}
	 *                                       operation is not supported by this iterator
	 * @throws IllegalStateException         if the {@code next} method has not
	 *                                       yet been called, or the {@code remove} method has already
	 *                                       been called after the last call to the {@code next}
	 *                                       method
	 */
	@Override
	public void remove()
	{
		iterator.remove();
	}

	protected void statusReport()
	{
		long totalDur = System.currentTimeMillis() - jobStartTime;
		long intervalDur = System.currentTimeMillis() - lastStatusReport;
		lastStatusReport = System.currentTimeMillis();

		logger.info("Status update for {}.{} scanning.  Since start: In {} ms, processed {} partition keys ({}/s), keeping {} and dropping {}.  Processed {} cluster keys ({}/s), keeping {} and dropping {}.",
			cfs.keyspace.getName(),
			cfs.getColumnFamilyName(),
			totalDur,
			keptPartitionKeys + droppedPartitionKeys,
			totalDur > 0 ? (keptPartitionKeys + droppedPartitionKeys) * 1000 / totalDur : "--",
			keptPartitionKeys,
			droppedPartitionKeys,
			keptClusterKeys + droppedClusterKeys,
			totalDur > 0 ? (keptClusterKeys + droppedClusterKeys) * 1000 / totalDur : "--",
			keptClusterKeys,
			droppedClusterKeys
			);

		long deltaPartition = keptPartitionKeys + droppedPartitionKeys - lastKeptPartitionKeys - lastDroppedPartitionKeys;
		long deltaCluster = keptClusterKeys + droppedClusterKeys - lastKeptClusterKeys - lastDroppedClusterKeys;
		logger.info("Status update for {}.{} scanning.  Since last update: In {} ms, processed {} partition keys ({}/s), keeping {} and dropping {}.  Processed {} cluster keys ({}/s), keeping {} and dropping {}.",
			cfs.keyspace.getName(),
			cfs.getColumnFamilyName(),
			intervalDur,
			deltaPartition,
			intervalDur > 0 ? deltaPartition * 1000 / intervalDur : "--",
			keptPartitionKeys - lastKeptPartitionKeys,
			droppedPartitionKeys - lastDroppedPartitionKeys,
			deltaCluster,
			intervalDur > 0 ? deltaCluster * 1000 / intervalDur : "--",
			keptClusterKeys - lastKeptClusterKeys,
			droppedClusterKeys - lastDroppedClusterKeys
			);

		lastDroppedClusterKeys = droppedClusterKeys;
		lastKeptClusterKeys = keptClusterKeys;
		lastDroppedPartitionKeys = droppedPartitionKeys;
		lastKeptPartitionKeys = keptPartitionKeys;
	}
}
