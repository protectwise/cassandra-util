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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public interface IDeletedRecordsSink extends AutoCloseable
{
	/**
	 * Accept an entire partition and all of its cells
	 * @param partition
	 */
	void accept(OnDiskAtomIterator partition);

	/**
	 * Accept just some cells of a partition
	 * @param key
	 * @param cell
	 */
	void accept(DecoratedKey key, OnDiskAtom cell);

	/**
	 * Open any resources related to sinking this data
	 */
	void begin();

	/**
	 * Close out any resources related to sinking this data
	 * Only one of close() or abort() should be called.
	 * @throws Exception
	 */
	void close() throws IOException;

	/**
	 * Abort the operation and discard any outstanding data.
	 * Only one of close() or abort() should be called.
	 */
	void abort();
}
