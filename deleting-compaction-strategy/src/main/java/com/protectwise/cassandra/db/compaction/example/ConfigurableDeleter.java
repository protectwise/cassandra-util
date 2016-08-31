/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.db.compaction.example;

import com.protectwise.cassandra.db.compaction.AbstractClusterDeletingConvictor;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class meant to provide an example of what a deleter that accepted configuration would look like.
 * Note that it is probably a BAD IDEA to use this class directly, or even borrow ideas from it very heavily
 * Basically you do not want to be modifying your schema when your rules for what to delete change, unless they
 * change very infrequently.  Engaging in frequent schema updates puts your cluster at risk of schema disagreement
 */
public class ConfigurableDeleter extends AbstractClusterDeletingConvictor
{
	private static final Logger logger = LoggerFactory.getLogger(ConfigurableDeleter.class);

	protected final static String DELETE_KEYS = "delete_keys";
	protected final Map<ByteBuffer, ByteBuffer[][]> rules;

	private static final ObjectMapper jsonMapper = new ObjectMapper();

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
		logger.warn("You are using an example deleting compaction strategy.  Direct production use of these classes is STRONGLY DISCOURAGED!");

		if (!options.containsKey(DELETE_KEYS))
		{
			throw new ConfigurationException(DELETE_KEYS + " is a required configuration property");
		}
		String value = options.get(DELETE_KEYS);
		try
		{
			Map<String, Object> rules = jsonMapper.readValue(value, new TypeReference<Map<String, Object>>()
			{
			});
			for (Map.Entry<String, Object> e : rules.entrySet())
			{
				// Maps are no bueno, they serve no purpose here.
				if (e.getValue() instanceof Map<?, ?>)
				{
					throw new ConfigurationException(e.getKey() + " contains an invalid value.  Only simple values or arrays are supported.");
				}
				else if (e.getValue() instanceof List<?>)
				{
					// First-level lists must contain only simple values or sub-lists
					for (Object v : (List<?>) e.getValue())
					{
						// Maps are still no bueno.
						if (v instanceof Map<?, ?>)
						{
							throw new ConfigurationException(e.getKey() + " contains an invalid value.  Only simple values or arrays are supported.");
						}
						else if (v instanceof List<?>)
						{
							if (((List<?>) v).size() != 2)
							{
								// Sub-lists must be exactly 2 elements long
								throw new ConfigurationException(e.getKey() + " contains an invalid value.  Sub arrays define a range, and must be exactly 2 elements long, indicating the lower and upper bound.");
							}

							// First level lists can still contain a second level list, but it stops here.
							for (Object v2 : (List<?>) v)
							{
								if (v2 instanceof Map<?, ?>)
								{
									// bueno-ness of maps still has not changed.
									throw new ConfigurationException(e.getKey() + " contains an invalid value.  Only simple values or arrays are supported.");
								}
								else if (v2 instanceof List<?>)
								{
									// A third level list enters no bueno territory.
									throw new ConfigurationException(e.getKey() + " contains an invalid value.  Arrays may contain simple values or one level of sub-array to indicate a range.");
								}
							}
						}
					}
				}
			}
		}
		catch (IOException e)
		{
			throw new ConfigurationException("Invalid JSON received for " + DELETE_KEYS, e);
		}
		options.remove(DELETE_KEYS);

		return options;
	}

	/**
	 * @param cfs
	 * @param options
	 */
	public ConfigurableDeleter(ColumnFamilyStore cfs, Map<String, String> options)
	{
		super(cfs, options);

		Map<String, Object> rules;
		try
		{
			rules = jsonMapper.readValue(options.get(DELETE_KEYS), new TypeReference<Map<String, Object>>()
			{
			});
		}
		catch (IOException e)
		{
			logger.error("Invalid JSON received for " + DELETE_KEYS, e);
			rules = new HashMap<>();
		}

		this.rules = new HashMap<>(rules.entrySet().size());
		for (Map.Entry<String, Object> e : rules.entrySet())
		{
			ColumnDefinition cdef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes(e.getKey()));
			ByteBuffer[][] ranges;
			if (e.getValue() instanceof List<?>)
			{
				ranges = new ByteBuffer[((List<?>) e.getValue()).size()][];
				int i = 0;

				for (Object outerEntry : (List<?>) e.getValue())
				{
					ByteBuffer[] range = new ByteBuffer[2];

					if (outerEntry instanceof List<?>)
					{
						assert ((List<?>) outerEntry).size() == 2 : "List entry is not of size 2.";
						// Validation has already proven this is a list of size 2.
						range[0] = parseRuleValue(cdef.type, ((List<?>) outerEntry).get(0));
						range[1] = parseRuleValue(cdef.type, ((List<?>) outerEntry).get(1));
					}
					else
					{
						// Validation has proven this is a simple value.
						range[0] = parseRuleValue(cdef.type, outerEntry);
						range[1] = range[0];
					}
					ranges[i++] = range;
				}
			}
			else
			{
				// Validation has proven this is a single simple value.
				ByteBuffer parsed = parseRuleValue(cdef.type, e.getValue());
				ranges = new ByteBuffer[][]{new ByteBuffer[]{parsed, parsed}};
			}
			if (ranges.length > 0)
			{
				this.rules.put(cdef.name.bytes, ranges);
			}
			logger.info("Rule: {} ({}) = {}", e.getKey(), e.getValue(), printRanges(ranges, cdef.type.getSerializer()));
		}

		logger.warn("You are using an example deleting compaction strategy.  Direct production use of these classes is STRONGLY DISCOURAGED!");
	}

	protected <T> String printRanges(ByteBuffer[][] ranges, TypeSerializer<T> ser)
	{
		if (ranges.length == 0)
		{
			return "none";
		}

		StringBuffer buf = new StringBuffer();
		buf.append("[");

		boolean first = true;
		for (ByteBuffer[] range : ranges)
		{
			if (!first) buf.append(", ");
			if (range[0] == null && range[1] == null)
			{
				buf.append("any");
			}
			else
			{
				buf.append("[");
				if (range[0] == null)
				{
					buf.append("*");
				}
				else
				{
					buf.append(ser.toString(ser.deserialize(range[0])));
				}
				buf.append(" - ");
				if (range[1] == null)
				{
					buf.append("*");
				}
				else
				{
					buf.append(ser.toString(ser.deserialize(range[1])));
				}
				buf.append("]");
			}
			first = false;
		}
		buf.append("]");
		return buf.toString();
	}

	protected ByteBuffer parseRuleValue(AbstractType<?> type, Object value)
	{
		if (value == null)
		{
			return null;
		}
		return type.fromString(value.toString());
	}

	/**
	 * Returns true if value is found inside one of the ranges defined.
	 *
	 * @param ranges
	 * @param value
	 * @return
	 */
	protected boolean testRule(ByteBuffer[][] ranges, ByteBuffer value)
	{
		if (value == null)
		{
			logger.warn("Null value");
			return false;
		}
		for (ByteBuffer[] range : ranges)
		{
			// Null values indicate unbounded.  range[0] is the lower bound inclusive, range[1] is the upper bound inclusive.
			// We are guaranteed both values exist before we get to this point.
			if (
					(range[0] == null || ByteBufferUtil.compareUnsigned(range[0], value) <= 0)
							&& (range[1] == null || ByteBufferUtil.compareUnsigned(range[1], value) >= 0)
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
		for (Map.Entry<ByteBuffer, ByteBuffer> e : this.getNamedPkColumns(key).entrySet())
		{
			if (rules.containsKey(e.getKey()))
			{
				// testRule returns true when it's found in the ranges, which means it's something we should delete
				// so we return the opposite of that.
				return !testRule(rules.get(e.getKey()), e.getValue());
			}
		}
		return true;
	}

	@Override
	public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name)
	{
		for (ColumnDefinition def : cfs.metadata.clusteringColumns())
		{
			if (rules.containsKey(def.name.bytes))
			{
				ByteBuffer[][] rule = rules.get(def.name.bytes);
				ByteBuffer value = this.getBytes(name, def);
				return !testRule(rule, value);
			}
		}
		return true;
	}
}
