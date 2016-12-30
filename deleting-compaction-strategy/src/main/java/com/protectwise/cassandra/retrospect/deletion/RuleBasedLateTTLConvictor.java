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
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class RuleBasedLateTTLConvictor extends AbstractClusterDeletingConvictor
{
	private static final Logger logger = LoggerFactory.getLogger(RuleBasedLateTTLConvictor.class);

	public final static String RULES_STATEMENT_KEY = "rules_select_statement";
	public final static String DEFAULT_TTL_KEY = "default_ttl";

	protected final String selectStatement;
	protected final List<Rule> rules;
	protected final Long defaultTTL;
	protected boolean isSpooked = false;
	protected boolean containsPartitionKeys;
	protected boolean containsClusterKeys;

	protected List<Rule> partitionRules;
	protected Long effectiveTTL = null;
	protected long fixedTtlBaseTime = System.currentTimeMillis();

	class Rule {
		protected final Map<ByteBuffer, ByteBuffer[][]> columnsAndRanges;
		protected final Long ttl;
		protected final boolean spooked;
		protected final String name;

		protected Rule(String name, Map<ByteBuffer, ByteBuffer[][]> columnsAndRanges, Long ttl)
		{
			if (columnsAndRanges.size() == 0)
			{
				logger.warn("Empty rule, would convict EVERYTHING.  Choosing to convict nothing instead.  If you really want to convict everything, enter a range of null to null.");
				spooked = true;
			}
			else
			{
				spooked = false;
			}
			this.columnsAndRanges = columnsAndRanges;
			this.ttl = ttl;
			this.name = name;
		}

		boolean containsKey(ByteBuffer key)
		{
			return columnsAndRanges.containsKey(key);
		}

		/**
		 * Returns true if value is found inside one of the ranges defined for that column.
		 *
		 * @param columnName
		 * @param value
		 * @return
		 */
		protected boolean testColumn(ByteBuffer columnName, ByteBuffer value)
		{
			if (value == null)
			{
				logger.warn("Null value");
				return false;
			}
			ByteBuffer[][] ranges = columnsAndRanges.get(columnName);
			AbstractType<?> comparator = cfs.metadata.getColumnDefinition(columnName).type;
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

		/**
		 * Tests whether a set of columns passes the definitions found in this rule
		 *
		 * For a given rule, the set of columns it defines is an AND condition, and the
		 * set of ranges for that column is an OR condition within that column.  The
		 * rule passes the column set if EVERY column matches at least ONE range.
		 *
		 * Columns which are not defined in the rule always pass, this method will always
		 * return true if there is no overlap between the provided columns and the ruleset.
		 *
		 * @param columns Column name and value for that column for a set of columns to test
		 * @return
		 */
		protected boolean testColumns(Map<ByteBuffer, ByteBuffer> columns)
		{
			for (Map.Entry<ByteBuffer, ByteBuffer> e : columns.entrySet())
			{
				// PK columns which are not defined in the rule are not considered
				if (!containsKey(e.getKey()))
				{
					continue;
				}

				if (!testColumn(e.getKey(), e.getValue()))
				{
					if (logger.isTraceEnabled())
					{
						logger.trace("Testing rule {} against columns {}=false", toString(), PrintHelper.printMap(columns));
					}
					return false;
				}
			}
			if (logger.isTraceEnabled())
			{
				logger.trace("Testing rule {} against columns {}=true", toString(), PrintHelper.printMap(columns));
			}
			return true;
		}

		@Override
		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			sb.append(name);
			sb.append('(');
			for (Map.Entry<ByteBuffer, ByteBuffer[][]> entry : columnsAndRanges.entrySet())
			{
				AbstractType<?> comparator = cfs.metadata.getColumnDefinition(entry.getKey()).type;
				sb.append(PrintHelper.printByteBuffer(entry.getKey()));
				for (ByteBuffer[] range : entry.getValue())
				{
					sb.append('[');
					sb.append(range[0] == null ? "*" : comparator.getString(range[0]));
					sb.append(',');
					sb.append(range[1] == null ? "*" : comparator.getString(range[1]));
					sb.append(']');
				}
				sb.append(" ");
			}
			sb.append(')');
			return sb.toString();
		}
	}


	/**
	 * @param cfs
	 * @param options
	 */
	public RuleBasedLateTTLConvictor(ColumnFamilyStore cfs, Map<String, String> options) throws ConfigurationException
	{
		super(cfs, options);
		selectStatement = options.get(RULES_STATEMENT_KEY);
		if (options.containsKey(DEFAULT_TTL_KEY))
		{
			try
			{
				defaultTTL = Long.parseLong(options.get(DEFAULT_TTL_KEY));
			}
			catch (NumberFormatException e)
			{
				throw new ConfigurationException("Invalid value '" + options.get(DEFAULT_TTL_KEY) + "' for " + DEFAULT_TTL_KEY, e);
			}
		} else {
			defaultTTL = null;
		}
		List<Rule> rules;
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

		logger.debug("Got {} rules to consider", rules.size());
	}

	/**
	 * Keyed by rulename, then by column name, then contains a list of 2-element arrays of ranges for that column.
	 * This is not typed, everything is byte buffers, type is collapsed at testing time.
	 *
	 * @param statement
	 * @return
	 * @throws ConfigurationException
	 */
	public static Map<ByteBuffer, Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long>> parseRules(String statement) throws ConfigurationException
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
			ConfigurationException ce = new ConfigurationException("Unable to query for rule data, the failed statement was " + statement, e);
			throw ce;
		}

		Map<String, ColumnSpecification> cols = new HashMap<>();
		for (ColumnSpecification cs : rawRuleData.metadata())
		{
			cols.put(cs.name.toString(), cs);
		}

		if (!cols.containsKey("column") || !cols.containsKey("rulename") || !cols.containsKey("range_lower") || !cols.containsKey("range_upper") || !cols.containsKey("ttl"))
		{
			throw new ConfigurationException("The select statement must return the columns 'column', 'rulename', 'range', and 'ttl'");
		}

		CQL3Type columnType = cols.get("column").type.asCQL3Type();
		if (!columnType.equals(CQL3Type.Native.TEXT))
		{
			throw new ConfigurationException("The 'column' column must be a text type.  Found " + columnType.toString());
		}

		//  Validate that "range" is of type tuple<text,text>, ugh.
		/*CQL3Type rangeType = cols.get("range").type.asCQL3Type();
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
		}*/

		// validate that range, range_lower, range_upper
                CQL3Type rangeLowerType = cols.get("range_lower").type.asCQL3Type();
		if(!rangeLowerType.equals(CQL3Type.Native.TEXT)) {
			throw new ConfigurationException("The column 'range_lower' must be of type text  Found " + cols.get("range_lower").type.getSerializer().getType());
		}

		CQL3Type rangeUpperType = cols.get("range_upper").type.asCQL3Type();
                if(!rangeLowerType.equals(CQL3Type.Native.TEXT)) {
                        throw new ConfigurationException("The column 'range' must be of type map<text,text>  Found " + cols.get("range_upper").type.getSerializer().getType());
                }

		// Validate that 'ttl' is of type bigint
		CQL3Type ttlType = cols.get("ttl").type.asCQL3Type();
		if (!ttlType.equals(CQL3Type.Native.BIGINT))
		{
			throw new ConfigurationException("The 'ttl' column must be a bigint type.  Found " + ttlType.toString());
		}

		Iterator<UntypedResultSet.Row> resultIterator = rawRuleData.iterator();

		Map<ByteBuffer, Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long>> rules = new HashMap<>();
		while (resultIterator.hasNext())
		{
			UntypedResultSet.Row row = resultIterator.next();
			ByteBuffer rulename = row.getBlob("rulename");
			Map<ByteBuffer, List<ByteBuffer[]>> rule;
			Long ttl = row.getLong("ttl");
			if (!rules.containsKey(rulename))
			{
				rule = new HashMap<>();
				rules.put(rulename, Pair.create(rule, ttl));
			}
			else
			{
				Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long> p = rules.get(rulename);
				if (!p.right.equals(ttl))
				{
					throw new ConfigurationException("The 'ttl' value for rule " + PrintHelper.bufToString(rulename) + " has inconsistent values between the columns and ranges of this rule.  The value of the TTL must be consistent for the entire rule.");
				}
				rule = p.left;
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
			ByteBuffer[] rawRange = new ByteBuffer[2];
			rawRange[0] = row.getBlob("range_lower");
			rawRange[1] =  row.getBlob("range_upper");
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
	protected List<Rule> translateRules(Map<ByteBuffer, Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long>> raw)
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

		List<Rule> rules = new ArrayList<>(raw.size());
		try
		{
			for (Map.Entry<ByteBuffer, Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long>> entry : raw.entrySet())
			{
				Pair<Map<ByteBuffer, List<ByteBuffer[]>>, Long> rule = entry.getValue();
				ByteBuffer ruleName = entry.getKey();

				Map<ByteBuffer, ByteBuffer[][]> parsedRule = new HashMap<>();
				Long ttl = rule.right;
				boolean ruleContainsPk = false;
				boolean ruleContainsCk = false;
				for (Map.Entry<ByteBuffer, List<ByteBuffer[]>> e : rule.left.entrySet())
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
						ruleContainsPk = true;
					}
					else if (isCk)
					{
						containsClusterKeys = true;
						ruleContainsCk = true;
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
				if (!ruleContainsPk && !ruleContainsCk)
				{
					isSpooked = true;
					logger.warn("Degrading to dry run: Rule {} contains neither a partition key nor a cluster key.  " +
							"This rule would apply to every record, which is probably not the intended effect.  " +
							"If you do want to write a catch-all rule, include a column with a range of null to null, " +
							"which will convict every value for that column instead.",
							PrintHelper.bufToString(ruleName));
				}
				rules.add(new Rule(PrintHelper.bufToString(ruleName), parsedRule, ttl));
			}
		}
		catch (Exception e1)
		{
			// Not sure this is actually possible to happen, we're reading pre-validated data.
			isSpooked = true;
			logger.error("Degrading to dry run: Something sincerely bogus went wrong parsing your rules.  One of the fields could not be read correctly.  Additionally, trying to log information related to that failed.", e1);
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

		String defaultTTL = options.get(DEFAULT_TTL_KEY);
		if (defaultTTL != null)
		{
			try
			{
				Long.parseLong(defaultTTL);
			}
			catch (NumberFormatException e)
			{
				throw new ConfigurationException("Invalid value '" + options.get(DEFAULT_TTL_KEY) + "' for " + DEFAULT_TTL_KEY, e);
			}
		}

		options.remove(DEFAULT_TTL_KEY);
		options.remove(RULES_STATEMENT_KEY);
		return options;
	}


	@Override
	public boolean shouldKeepPartition(OnDiskAtomIterator key)
	{
		effectiveTTL = null;
		// Short circuit - if there are no partition keys, then the partition test MUST pass, and we'd reach the same
		// result with a lot more work.
		if (!containsPartitionKeys)
		{
			partitionRules = rules;
			return true;
		}

		partitionRules = new ArrayList<>();
		Map<ByteBuffer, ByteBuffer> namedPkColumns = getNamedPkColumns(key);

		for (Rule rule : rules)
		{
			if (rule.testColumns(namedPkColumns))
			{
				partitionRules.add(rule);
				if (!containsClusterKeys)
				{
					logger.trace("Partition only ruleset, partition determines the TTL: {}", rule.ttl);
					// we pull TTL from the matched partition rules
					if (effectiveTTL == null || rule.ttl < effectiveTTL)
					{
						effectiveTTL = rule.ttl;
					}
				}
			}
		}
		if (logger.isTraceEnabled())
		{
			logger.trace("Partition {} being considered against {} rules", PrintHelper.print(key, cfs), partitionRules.size());
		}
		return true;
	}

	@Override
	public boolean shouldKeepAtom(OnDiskAtomIterator partition, OnDiskAtom atom)
	{
		// Short circuit - if there are no cluster keys defined in the rules, then the partition code will
		// already have decided the correct TTL time
		if (containsClusterKeys)
		{
			// super.shouldKeepAtom maintains the state on cluster keys, calling into shouldKeepCluster
			// when appropriate, so that our cluster's TTL is correctly maintained.
			super.shouldKeepAtom(partition, atom);
		}

		// If effectiveTTL is null at this point, it's because no rules hit, we fall back to the default TTL if it was set.
		if (effectiveTTL == null)
		{
			if (logger.isTraceEnabled())
			{
				logger.trace("Falling back to default ttl ({}) for partition {}:{}",  defaultTTL, PrintHelper.print(partition, cfs), PrintHelper.print(atom, cfs));
			}
			effectiveTTL = defaultTTL;
		}

		if (effectiveTTL == null)
		{
			if (logger.isTraceEnabled())
			{
				logger.trace("No TTL found for {}:{}.  Passing atom", PrintHelper.print(partition, cfs), PrintHelper.print(atom, cfs));
			}
			// No rules matched either the partition or cluster key, we keep every record
			return true;
		}

		// To be consistent with statement level TTL, sub-zero TTL means no TTL.
		// This also produces the handy effect of letting you define a rule that overrides
		// other rules to indicate that a more narrowly defined record should be kept
		// even if other rules say it should be deleted, since we look for the lowest matching
		// TTL of all rules.
		if (effectiveTTL < 0)
		{
			if (logger.isTraceEnabled())
			{
				logger.trace("TTL is less than zero for {}:{}.  Passing atom", PrintHelper.print(partition, cfs), PrintHelper.print(atom, cfs));
			}
			return true;
		}

		// Compute the number of seconds ago that the record was written.  Default timestamps
		// are written in nanosecond scale, first convert to milliseconds to be consistent with
		// system time, then compute the age in seconds to compare to the TTL.
		final long atomTimestampInMillis = atom.timestamp() / 1000;
		final long recordAgeInSeconds = (this.fixedTtlBaseTime - atomTimestampInMillis) / 1000;
		if (recordAgeInSeconds > effectiveTTL)
		{
			if (logger.isTraceEnabled())
			{
				logger.trace("Atom age {} greater than effective TTL of {}, deleting {}:{}.", recordAgeInSeconds, effectiveTTL, PrintHelper.print(partition, cfs), PrintHelper.print(atom, cfs));
			}
			return false;
		}
		if (logger.isTraceEnabled())
		{
			logger.trace("Atom age {} lesser than effective TTL of {}, keeping {}:{}.", recordAgeInSeconds, effectiveTTL, PrintHelper.print(partition, cfs), PrintHelper.print(atom, cfs));
		}
		return true;
	}

	@Override
	public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name)
	{
		if (logger.isTraceEnabled())
		{
			logger.trace("Considering {} rules against the cluster key", partitionRules.size());
		}
		// We've already tested for the presence of cluster keys in the ruleset from within shouldKeepAtom()
		for (Rule rule : partitionRules)
		{
			if (rule.testColumns(getClusteringValues(name)))
			{
				// Keep the smallest TTL of the matched rules.
				if (effectiveTTL == null || rule.ttl < effectiveTTL)
				{
					effectiveTTL = rule.ttl;
				}
			}
		}
		return true;
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
