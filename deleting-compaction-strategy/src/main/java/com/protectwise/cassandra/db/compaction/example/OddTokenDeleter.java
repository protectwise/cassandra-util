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
package com.protectwise.cassandra.db.compaction.example;

import com.protectwise.cassandra.db.compaction.AbstractClusterDeletingConvictor;
import com.protectwise.cassandra.db.compaction.AbstractSimpleDeletingConvictor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Deletes partition keys whose <i>token</i> is an odd number
 */
public class OddTokenDeleter extends AbstractSimpleDeletingConvictor
{
	private static final Logger logger = LoggerFactory.getLogger(OddTokenDeleter.class);

	/**
	 * @param cfs
	 * @param options
	 */
	public OddTokenDeleter(ColumnFamilyStore cfs, Map<String, String> options)
	{
		super(cfs, options);
		logger.warn("You are using an example deleting compaction strategy.  Direct production use of these classes is STRONGLY DISCOURAGED!");
	}

	@Override
	public boolean shouldKeepPartition(OnDiskAtomIterator key)
	{
		Token token = key.getKey().getToken();
		if (token instanceof LongToken)
		{
			return ((LongToken) token).getTokenValue() % 2 == 0;
		}
		else
		{
			logger.info(String.format(
					"Unable to get an integer position from %s", token.getClass().getCanonicalName()
			));
			return true;
		}
	}

}
