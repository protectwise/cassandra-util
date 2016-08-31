/*
 * Copyright 2016 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.db.compaction.example;

import com.protectwise.cassandra.db.compaction.AbstractClusterDeletingConvictor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class AllClusterKeyDeleter extends AbstractClusterDeletingConvictor
{
	private static final Logger logger = LoggerFactory.getLogger(OddClusterKeyDeleter.class);

	/**
	 * @param cfs
	 * @param options
	 */
	public AllClusterKeyDeleter(ColumnFamilyStore cfs, Map<String, String> options)
	{
		super(cfs, options);
		logger.warn("You are using an example deleting compaction strategy.  Direct production use of these classes is STRONGLY DISCOURAGED!");
	}

	@Override
	public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name)
	{
		return false;
	}

	@Override
	public boolean shouldKeepPartition(OnDiskAtomIterator key)
	{
		return true;
	}

}
