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

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;

import com.protectwise.cassandra.io.sstable.FilteringSSTableScanner;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeletingCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(DeletingCompactionStrategy.class);

    private final DeletingCompactionStrategyOptions options;
    private final AbstractCompactionStrategy underlying;

    public DeletingCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) throws Exception
    {
        super(cfs, options);
        this.options = new DeletingCompactionStrategyOptions(cfs, options);
        this.underlying = this.options.underlying;
        if (this.underlying == null)
        {
            throw new Exception("Unable to instantiate underlying compaction strategy, deleting compaction strategy cannot be started");
        }
        logger.debug("Created DeletionCompactionStrategy");
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options = AbstractCompactionStrategy.validateOptions(options);
        logger.debug("Validated underlying compaction startegy options");
        Map<String, String> options2 =  DeletingCompactionStrategyOptions.validateOptions(options);
        logger.debug("Deletion compaction startegy validated options");
        return options2;
    }

    @Override
    public boolean shouldBeEnabled()
    {
        // This happens during construction because shouldBeEnabled() is called by AbstractCompactionStrategy.<init>
        // There doesn't seem to be a strongly better option here without double-invoking the options and/or constructor
        // or juggling variables into temp locations.  As of right now, no core compaction strategies overload this
        // method, so the two forks of this clause are effectively identical.
        if (underlying == null)
        {
	        return super.shouldBeEnabled();
        }
        else
        {
            return underlying.shouldBeEnabled();
        }
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
        {
            return null;
        }
        return underlying.getNextBackgroundTask(gcBefore);
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     *
     * @param sstables
     * @param range
     */
    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        ScannerList scanners = underlying.getScanners(sstables, range);
        List<ISSTableScanner> filteredScanners = new ArrayList<ISSTableScanner>(scanners.scanners.size());
        AbstractSimpleDeletingConvictor convictor = options.buildConvictor();
        for (ISSTableScanner scanner : scanners.scanners)
        {
            IDeletedRecordsSink backupSink = null;
            if (options.deletedRecordsSinkDirectory != null)
            {
                backupSink = new BackupSinkForDeletingCompaction(cfs, options.deletedRecordsSinkDirectory, options.kafkaQueueBootstrapServers, options.cassandraPurgedKafkaTopic);
            }
            filteredScanners.add(new FilteringSSTableScanner(
                    scanner,
                    convictor,
                    convictor,
                    cfs,
                    options.dryRun || convictor.isDryRun(),
                    backupSink,
                    options.statsReportInterval
            ));
        }
        return new ScannerList(filteredScanners);
    }

    @Override
    public boolean shouldDefragment()
    {
        return underlying.shouldDefragment();
    }

    @Override
    public String getName()
    {
        return underlying.getName();
    }

    @Override
    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        underlying.replaceSSTables(removed, added);
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore)
    {
        return underlying.getMaximalTask(gcBefore);
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return underlying.getUserDefinedTask(sstables, gcBefore);
    }

    @Override
    public AbstractCompactionTask getCompactionTask(Collection<SSTableReader> sstables, int gcBefore, long maxSSTableBytes)
    {
        return underlying.getCompactionTask(sstables, gcBefore, maxSSTableBytes);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return underlying.getEstimatedRemainingTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return underlying.getMaxSSTableBytes();
    }

    @Override
    public boolean isEnabled()
    {
        return underlying.isEnabled();
    }

    @Override
    public void enable()
    {
        underlying.enable();
    }

    @Override
    public void disable()
    {
        underlying.disable();
    }

    /**
     * @return whether or not MeteredFlusher should be able to trigger memtable flushes for this CF.
     */
    @Override
    public boolean isAffectedByMeteredFlusher()
    {
        return underlying.isAffectedByMeteredFlusher();
    }

    /**
     * If not affected by MeteredFlusher (and handling flushing on its own), override to tell MF how much
     * space to reserve for this CF, i.e., how much space to subtract from `memtable_total_space_in_mb` when deciding
     * if other memtables should be flushed or not.
     */
    @Override
    public long getMemtableReservedSize()
    {
        return underlying.getMemtableReservedSize();
    }

    /**
     * Handle a flushed memtable.
     *  @param memtable the flushed memtable
     * @param sstable the written sstable. can be null if the memtable was clean.
     */
    @Override
    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        underlying.replaceFlushed(memtable, sstable);
    }

    /**
     * @return a subset of the suggested sstables that are relevant for read requests.
     * @param sstables
     */
    @Override
    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        return underlying.filterSSTablesForReads(sstables);
    }

    /**
     * Filters SSTables that are to be blacklisted from the given collection
     *
     * @param originalCandidates The collection to check for blacklisted SSTables
     * @return list of the SSTables with blacklisted ones filtered out
     */
    public static Iterable<SSTableReader> filterSuspectSSTables(Iterable<SSTableReader> originalCandidates)
    {
        return AbstractCompactionStrategy.filterSuspectSSTables(originalCandidates);
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        underlying.addSSTable(added);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        underlying.removeSSTable(sstable);
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> toCompact)
    {
        return underlying.getScanners(toCompact);
    }

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    @Override
    public void pause()
    {
        underlying.pause();
    }

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    @Override
    public void resume()
    {
        underlying.resume();
    }

    /**
     * Performs any extra initialization required
     */
    @Override
    public void startup()
    {
        underlying.startup();
    }

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     */
    @Override
    public void shutdown()
    {
        underlying.shutdown();
    }
}
