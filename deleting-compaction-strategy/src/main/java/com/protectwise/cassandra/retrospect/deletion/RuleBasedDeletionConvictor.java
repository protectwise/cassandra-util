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
package com.protectwise.cassandra.retrospect.deletion;

import com.protectwise.cassandra.db.compaction.AbstractClusterDeletingConvictor;
import com.protectwise.cassandra.util.PrintHelper;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class RuleBasedDeletionConvictor extends AbstractClusterDeletingConvictor
{
	private static final Logger logger = LoggerFactory.getLogger(RuleBasedDeletionConvictor.class);

	public final static String RULES_STATEMENT_KEY = "rules_select_statement";

	protected final String selectStatement;
	protected final List<Map<ByteBuffer, ByteBuffer[][]>> rules;
	protected boolean isSpooked = false;
	protected boolean containsPartitionKeys;
	protected boolean containsClusterKeys;
	/**
	 * @param cfs
	 * @param options
	 */
	public RuleBasedDeletionConvictor(ColumnFamilyStore cfs, Map<String, String> options) throws ConfigurationException
	{
		super(cfs, options);
		selectStatement = options.get(RULES_STATEMENT_KEY);
		List<Map<ByteBuffer, ByteBuffer[][]>> rules;
		try
		{
			rules = translateRules(parseRules(selectStatement));
		}
		catch (ConfigurationException e)
		{
			// If we haven't fully started up before compaction begins, this error is expected because we can't
			// necessarily query the rules table.  Try to avoid logging errors at startup, however outside of startup
			// this should be a noisy exception.
			if (!QueryHelper.hasStartedCQL())
			{
				rules = new ArrayList<>(0);
				isSpooked = true;
				logger.info("Unable to query for deletion rules data, however it looks like this node has not fully joined the ring, so defaulting to a dry run.");
			}
			else
			{
				throw e;
			}
		}
		this.rules = rules;
		logger.info("Got {} rules to consider", rules.size());
	}

	/**
	 * Keyed by rulename, then by column name, then contains a list of 2-element arrays of ranges for that column.
	 * This is not typed, everything is byte buffers, type is collapsed at testing time.
	 *
	 * @param statement
	 * @return
	 * @throws ConfigurationException
	 */
	public static Map<ByteBuffer, Map<ByteBuffer, List<ByteBuffer[]>>> parseRules(String statement) throws ConfigurationException
	{
		UntypedResultSet rawRuleData = null;
		try
		{
			if (!QueryHelper.hasStartedCQL())
			{
				// Yuck, exceptions for control flow.  This will be caught upstream during compaction as a signal that
				// we should move to spooked mode.  Outside of compaction the exception will bubble up and be presented
				// to the user (though it seems extremely unlikely)
				throw new ConfigurationException("Node is not fully joined, so we cannot read deletion rules.  Falling back to standard compaction");
			}
			rawRuleData = QueryProcessor.process(statement, ConsistencyLevel.LOCAL_QUORUM);
		}
		catch (RequestExecutionException e)
		{
			ConfigurationException ce = new ConfigurationException("Unable to query for rule data.  The failed statement was " + statement, e);
			throw ce;
		}

		Map<String, ColumnSpecification> cols = new HashMap<>();
		for (ColumnSpecification cs : rawRuleData.metadata())
		{
			cols.put(cs.name.toString(), cs);
		}

		if (!cols.containsKey("column") || !cols.containsKey("rulename") || !cols.containsKey("range"))
		{
			throw new ConfigurationException("The select statement must return the columns 'column', 'rulename', and 'range'");
		}

		CQL3Type columnType = cols.get("column").type.asCQL3Type();
		if (!columnType.equals(CQL3Type.Native.TEXT))
		{
			throw new ConfigurationException("The 'column' column must be a text type.  Found " + columnType.toString());
		}

		//  Validate that "range" is of type tuple<text,text>, ugh.
		CQL3Type rangeType = cols.get("range").type.asCQL3Type();
		if (!(rangeType instanceof CQL3Type.Tuple))
		{
			throw new ConfigurationException("The column 'range' must be of type tuple<text,text>  Found " + cols.get("column").type.getSerializer().getType());
		}
		List<AbstractType<?>> subtypes = ((TupleType) ((CQL3Type.Tuple) rangeType).getType()).allTypes();
		if (subtypes.size() != 2)
		{
			throw new ConfigurationException("The column 'range' must be of type tuple<text,text>  Found " + cols.get("column").type.getSerializer().getType());
		}
		for (AbstractType<?> t : subtypes)
		{
			if (!t.asCQL3Type().equals(CQL3Type.Native.TEXT))
			{
				throw new ConfigurationException("The column 'range' must be of type tuple<text,text>  Found " + cols.get("column").type.getSerializer().getType());
			}
		}

		Iterator<UntypedResultSet.Row> resultIterator = rawRuleData.iterator();

		Map<ByteBuffer, Map<ByteBuffer, List<ByteBuffer[]>>> rules = new HashMap<>();
		while (resultIterator.hasNext())
		{
			UntypedResultSet.Row row = resultIterator.next();
			ByteBuffer rulename = row.getBlob("rulename");
			Map<ByteBuffer, List<ByteBuffer[]>> rule;
			if (!rules.containsKey(rulename))
			{
				rule = new HashMap<>();
				rules.put(rulename, rule);
			}
			else
			{
				rule = rules.get(rulename);
			}

			ByteBuffer column = row.getBlob("column");
			List<ByteBuffer[]> ranges;
			if (rule.containsKey(column))
			{
				ranges = rule.get(column);
			}
			else
			{
				ranges = new ArrayList<>();
				rule.put(column, ranges);
			}
			ByteBuffer[] rawRange = ((TupleType) rangeType.getType()).split(row.getBlob("range"));
			ranges.add(rawRange);
			if (logger.isDebugEnabled())
			{
				logger.debug("Rule {} on column {} is range {} to {} (now {} ranges on this column)",
					PrintHelper.bufToString(rulename),
					PrintHelper.bufToString(column),
					PrintHelper.bufToString(rawRange[0]),
					PrintHelper.bufToString(rawRange[1]),
					ranges.size()
				);
			}
		}


		return rules;
	}

	/**
	 * This function further refines knowledge from the basic rule parsing
	 * (which is invoked from a static context with no knowledge of what column family it's being invoked on) by
	 * further validating data, including:
	 *  - making sure that all the columns defined are actually testable (part of partition or cluster keys)
	 *  - testing for some short circuits on partition-only or cluster-only rulesets to make testing faster
	 *  - validating all the data is parseable
	 * @param raw
	 * @return
	 */
	protected List<Map<ByteBuffer, ByteBuffer[][]>> translateRules(Map<ByteBuffer, Map<ByteBuffer, List<ByteBuffer[]>>> raw)
	{
		containsPartitionKeys = false;
		containsClusterKeys = false;

		Set<ByteBuffer> partitionKeyColumnNames = new HashSet<>();
		for (ColumnDefinition pk : cfs.metadata.partitionKeyColumns())
		{
			partitionKeyColumnNames.add(pk.name.bytes);
		}

		Set<ByteBuffer> clusterKeyColumnNames = new HashSet<>();
		for (ColumnDefinition ck : cfs.metadata.clusteringColumns())
		{
			clusterKeyColumnNames.add(ck.name.bytes);
		}

		List<Map<ByteBuffer, ByteBuffer[][]>> rules = new ArrayList<>(raw.size());
		try
		{
			for (Map<ByteBuffer, List<ByteBuffer[]>> rule : raw.values())
			{
				Map<ByteBuffer, ByteBuffer[][]> parsedRule = new HashMap<>();
				for (Map.Entry<ByteBuffer, List<ByteBuffer[]>> e : rule.entrySet())
				{
					ByteBuffer columnName = e.getKey();
					ColumnDefinition col = cfs.metadata.getColumnDefinition(columnName);

					if (col == null)
					{
						isSpooked = true;
						logger.warn("Degrading to dry run: Column {} is not found",
							PrintHelper.bufToString(columnName)
						);
					}
					else if (!col.isPrimaryKeyColumn())
					{
						isSpooked = true;
						logger.warn("Degrading to dry run: Column {} is not part of the primary or clustering keys, it's not possible to use this column for conviction rules",
							PrintHelper.bufToString(columnName)
						);
					}

					boolean isPk = partitionKeyColumnNames.contains(col.name.bytes);
					boolean isCk = clusterKeyColumnNames.contains(col.name.bytes);
					if (isPk)
					{
						containsPartitionKeys = true;
					}
					else if (isCk)
					{
						containsClusterKeys = true;
					}

					ByteBuffer[][] ranges = new ByteBuffer[e.getValue().size()][2];
					int i = 0;
					for (ByteBuffer[] range : e.getValue())
					{
						try
						{
							// bounds on range are assumed to have been prevalidated by parseRules()
							if (range[0] != null)
							{
								ranges[i][0] = col.type.fromString(ByteBufferUtil.string(range[0]));
							}
							if (range[1] != null)
							{
								ranges[i][1] = col.type.fromString(ByteBufferUtil.string(range[1]));
							}
							if (logger.isDebugEnabled())
							{
								logger.debug("Col {} (pk? {}, ck?{}) from {} to {}",
									PrintHelper.bufToString(col.name.bytes),
									isPk,
									isCk,
									ranges[i][0] == null ? "*" : col.type.getString(ranges[i][0]),
									ranges[i][1] == null ? "*" : col.type.getString(ranges[i][1])
								);
							}
							i++;
						}
						catch (MarshalException me)
						{
							isSpooked = true;
							logger.warn("Degrading to dry run: Column {} contains an unparseable range ({} to {})",
								PrintHelper.bufToString(columnName),
								PrintHelper.bufToString(range[0]),
								PrintHelper.bufToString(range[1]),
								me
							);
						}
					}
					parsedRule.put(columnName, ranges);
				}
				rules.add(parsedRule);
			}
		}
		catch (Exception e1)
		{
			// Not sure this is actually possible to happen, we're reading pre-validated data.
			isSpooked = true;
			logger.error("Degrading to dry run: Something sincerely bogus went wrong parsing your rules.  One of the fields could not be read correctly.  Additionally, trying to log information related to that failed.", e1);
		}

		if (!containsPartitionKeys)
		{
			logger.warn("Ruleset for {} contains no partition keys.  Deleting compaction will have no effect.", cfs.keyspace.getName() + "." + cfs.getColumnFamilyName());
		}

		if (containsPartitionKeys && containsClusterKeys)
		{
			isSpooked = true;
			logger.error("Degrading to dry run: Ruleset contains both partition keys and cluster keys.  As of right now, this convictor can consider only one or the other.");
		}

		return rules;
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
		String select = options.get(RULES_STATEMENT_KEY);
		if (select == null)
		{
			throw new ConfigurationException(RULES_STATEMENT_KEY + " is a required parameter");
		}
		try
		{
			ParsedStatement stmt = QueryProcessor.parseStatement(select);
			if (!(stmt instanceof SelectStatement.RawStatement))
			{
				throw new ConfigurationException(RULES_STATEMENT_KEY + " must be a SELECT statement");
			}
			SelectStatement.RawStatement sel = (SelectStatement.RawStatement) stmt;

			try
			{
				sel.keyspace();
			}
			catch (AssertionError e)
			{
				throw new ConfigurationException(RULES_STATEMENT_KEY + " must define a fully qualified keyspace.tablename");
			}

			// This will validate that the data types of the columns in the select are correct.
			parseRules(select);
		}
		catch (SyntaxException e)
		{
			throw new ConfigurationException("Invalid select statement: " + e.getMessage(), e);
		}

		options.remove(RULES_STATEMENT_KEY);
		return options;
	}

	/**
	 * Returns true if value is found inside one of the ranges defined.
	 *
	 * @param ranges
	 * @param value
	 * @return
	 */
	protected <T> boolean testRule(ByteBuffer[][] ranges, ByteBuffer value, AbstractType<?> comparator)
	{
		if (value == null)
		{
			logger.warn("Null value");
			return false;
		}
		for (ByteBuffer[] range : ranges)
		{
			if (logger.isTraceEnabled())
			{
				logger.trace("Checking {} against {} to {}",
					comparator.getString(value),
					range[0] == null ? "*" : comparator.getString(range[0]),
					range[1] == null ? "*" : comparator.getString(range[1])
					);
			}
			// Null values indicate unbounded.  range[0] is the lower bound inclusive, range[1] is the upper bound inclusive.
			// We are guaranteed both values exist before we get to this point.
			if (   (range[0] == null || comparator.compare(range[0], value) <= 0)
				&& (range[1] == null || comparator.compare(range[1], value) >= 0)
				)
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean shouldKeepPartition(OnDiskAtomIterator key)
	{
		// Short circuit - if there are no partition keys, then the partition test MUST pass, and we'd reach the same
		// result with a lot more work.
		if (!containsPartitionKeys)
		{
			return true;
		}

		Map<ByteBuffer, ByteBuffer> namedPkColumns = getNamedPkColumns(key);

		for (Map<ByteBuffer, ByteBuffer[][]> rule : rules)
		{
			if (rule.size() == 0)
			{
				logger.warn("Empty rule, would convict EVERYTHING.  Choosing to convict nothing instead.  If you really want to convict everything, enter a range of null to null.");
				continue;
			}
			// For a given rule, the set of columns it defines is an AND condition, and the set of ranges for
			// that column are an OR condition.  The whole partition fails if for _one_ rule, _all_ columns fail.
			boolean allColumnsFail = true;

			for (Map.Entry<ByteBuffer, ByteBuffer> e : namedPkColumns.entrySet())
			{
				ColumnDefinition cdef = cfs.metadata.getColumnDefinition(e.getKey());

				// PK columns which are not defined in the rule are not considered
				if (!rule.containsKey(e.getKey()))
				{
					continue;
				}

				// testRule returns true when it's found in the ranges, which means it's something we should delete
				// so we return the opposite of that.
				boolean thisColumnPasses = !testRule(rule.get(e.getKey()), e.getValue(), cdef.type);

				if (thisColumnPasses)
				{
					allColumnsFail = false;
					break;
				}
			}

			if (allColumnsFail)
			{
				// This record gets deleted.
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean shouldKeepAtom(OnDiskAtomIterator partition, OnDiskAtom atom)
	{
		// Short circuit - if there are no cluster keys defined in the rules, then the cluster test MUST pass,
		// and we'd reach the same result with a lot more work
		if (!containsClusterKeys)
		{
			return true;
		}
		// This tests and maintains state for cluster keys
		return super.shouldKeepAtom(partition, atom);
	}

	@Override
	public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name)
	{
		// We've already tested for the presence of cluster keys in the ruleset from within shouldKeepAtom()
		for (Map<ByteBuffer, ByteBuffer[][]> rule : rules)
		{
			boolean allColumnsFail = true;
			for (ColumnDefinition def : cfs.metadata.clusteringColumns())
			{
				if (rule.containsKey(def.name.bytes))
				{
					boolean passes = !testRule(rule.get(def.name.bytes), getBytes(name, def), def.type);
					if (!passes) {
						allColumnsFail = false;
						break;
					}
				}
			}

			if (allColumnsFail)
			{
				return false;
			}
		}
		return true;
//		throw new UnsupportedOperationException("Not yet implemented: rules based testing of cluster columns.");
	}

	/**
	 * Allow a convictor to declare that it's in dry run mode.
	 *
	 * @return
	 */
	@Override
	public boolean isDryRun()
	{
		return isSpooked;
	}
}
