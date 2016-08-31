/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.io.sstable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;

public interface ISSTableScannerFilter {

    boolean shouldKeepPartition(OnDiskAtomIterator key);

}
