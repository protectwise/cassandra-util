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
package com.protectwise.cassandra.db.columniterator;

import com.google.common.base.Predicate;
import com.protectwise.cassandra.db.compaction.IDeletedRecordsSink;
import com.protectwise.cassandra.util.PrintHelper;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import com.google.common.collect.Iterators;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FilteringOnDiskAtomIterator implements OnDiskAtomIterator {
    private static final Logger logger = LoggerFactory.getLogger(FilteringOnDiskAtomIterator.class);

    protected final OnDiskAtomIterator underlying;
    protected final IOnDiskAtomFilter filter;
    protected final Iterator<OnDiskAtom> iterator;
    protected final ColumnFamilyStore cfs;
    protected final List<Cell> indexedColumnsInRow;
    protected final boolean dryRun;

    public long kept = 0;
    public long filtered = 0;

    public FilteringOnDiskAtomIterator(
            final OnDiskAtomIterator underlying,
            IOnDiskAtomFilter filter,
            final ColumnFamilyStore cfs,
            final boolean dryRun,
            final IDeletedRecordsSink backupSink
    ) {
        this.underlying = underlying;
        this.filter = filter;
        this.cfs = cfs;
        this.dryRun = dryRun;

        final boolean hasIndexes = cfs.indexManager.hasIndexes();
        if (hasIndexes) this.indexedColumnsInRow = new ArrayList<Cell>();
        else this.indexedColumnsInRow = null;


        iterator = Iterators.filter(underlying, new Predicate<OnDiskAtom>()
        {
            @Override
            public boolean apply(OnDiskAtom input)
            {
                if (FilteringOnDiskAtomIterator.this.filter.shouldKeepAtom(underlying, input))
                {
                    kept += 1;
                    return true;
                }
                else if (dryRun)
                {
                    // Dry run, we're not deleting anything, but record that we would have done so.
                    filtered += 1;
                    return true;
                }
                else
                {
                    if (backupSink != null)
                    {
                        backupSink.accept(underlying.getKey(), input);
                    }
                    // Actually delete the record
                    // TODO: BUG: THERE IS A BUG HERE,
                    // Records with an indexed clustered key which were created with UPDATE rather than INSERT
                    // will not have the clustered key's index cleaned up correctly.  This seems to be a problem found
                    // even in vanilla Cassandra's cleanup routine.  When INSERTs are done, a boundary cell is written
                    // which becomes responsible for cleaning up the cluster keys during cleanup style operations (which
                    // deleting compaction shares much in common with).  During UPDATE no such boundary cell is written,
                    // which doesn't leave the appropriate value to trigger index cleanup on.
                    // To address that here, we'd need to manufacture a synthetic boundary cell and convict it
                    // for cluster keys that are convicted.  Forcing a reindex corrects this, but of course reindexing
                    // after deletes is I/O prohibitive.  I suspect but have not tested that this will affect TTLd
                    // columns as well.
                    if (hasIndexes && input instanceof Cell && cfs.indexManager.indexes((Cell) input))
                    {
                        indexedColumnsInRow.add((Cell) input);
                    }
                    filtered += 1;
                    return false;
                }
            }
        });
    }

    /**
     * @return A ColumnFamily holding metadata for the row being iterated.
     * Do not modify this CF. Whether it is empty or not is implementation-dependent.
     */
    @Override
    public ColumnFamily getColumnFamily() {
        ColumnFamily underlyingColumnFamily = underlying.getColumnFamily();
        DeletionTime topLevelDeletion = underlyingColumnFamily.deletionInfo().getTopLevelDeletion();
        if (filter.shouldKeepTopLevelDeletion(underlying, topLevelDeletion))
        {
          return underlyingColumnFamily;
        }
        else
        {
          return ArrayBackedSortedColumns.factory.create(underlyingColumnFamily.metadata());
        }
    }

    /**
     * @return the current row key
     */
    @Override
    public DecoratedKey getKey() {
        return underlying.getKey();
    }

    /** clean up any open resources */
    @Override
    public void close() throws IOException
    {
        underlying.close();
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
        boolean hn = iterator.hasNext();
        if (!hn && indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
        {
            // acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
            try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start())
            {
                cfs.indexManager.deleteFromIndexes(underlying.getKey(), indexedColumnsInRow, opGroup);
            }

        }
        return hn;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public OnDiskAtom next() {
        return iterator.next();
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
     *         operation is not supported by this iterator
     *
     * @throws IllegalStateException if the {@code next} method has not
     *         yet been called, or the {@code remove} method has already
     *         been called after the last call to the {@code next}
     *         method
     */
    @Override
    public void remove() {
        iterator.remove();
    }
}
