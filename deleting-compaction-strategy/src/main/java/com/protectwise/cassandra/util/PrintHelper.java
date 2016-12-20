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
package com.protectwise.cassandra.util;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrintHelper
{
	private static final Logger logger = LoggerFactory.getLogger(PrintHelper.class);

	public static String print(OnDiskAtomIterator oda, ColumnFamilyStore cfs) {
		StringWriter w = new StringWriter();
		print(oda, cfs, w);
		return w.toString();
	}
	public static void print(OnDiskAtomIterator oda, ColumnFamilyStore cfs, StringWriter w) {
		ByteBuffer[] keyParts;
		AbstractType<?> validator = cfs.metadata.getKeyValidator();
		if (validator instanceof CompositeType)
		{
			keyParts = ((CompositeType) validator).split(oda.getKey().getKey());
		}
		else
		{
			keyParts = new ByteBuffer[]{
					oda.getKey().getKey()
			};
		}
		List<ColumnDefinition> pkc = cfs.metadata.partitionKeyColumns();
		for (ColumnDefinition def : pkc)
		{
			w.write(bufToString(def.name.bytes));
			w.write(':');
			w.write(def.type.getSerializer().deserialize(keyParts[def.position()]).toString());
		}
	}

	public static String print(OnDiskAtomIterator oda, OnDiskAtom i, ColumnFamilyStore cfs) {
		StringWriter w = new StringWriter();
		print(oda, i, cfs, w);
		return w.toString();
	}
	public static void print(OnDiskAtomIterator oda, OnDiskAtom i, ColumnFamilyStore cfs, StringWriter w)
	{
		print(oda, cfs, w);
		w.write(',');
		print(i, cfs, w);
	}

	public static String print(OnDiskAtom i, ColumnFamilyStore cfs) {
		StringWriter w = new StringWriter();
		print(i, cfs, w);
		return w.toString();
	}
	public static void print(OnDiskAtom i, ColumnFamilyStore cfs, StringWriter w) {

		for ( ColumnDefinition def : cfs.metadata.clusteringColumns() )
		{
			w.write(bufToString(def.name.bytes));
			w.write(':');
			w.write(def.type.getSerializer().deserialize(i.name().get(def.position())).toString());
			w.write(',');
		}

		if (i instanceof Cell) {
			ColumnDefinition col = cfs.metadata.getColumnDefinition(((Cell)i).name());
			if (col != null) {
				w.write(bufToString(col.name.bytes));
				w.write(':');
				w.write(col.type.getSerializer().deserialize(((Cell)i).value()).toString());
			} else {
				w.write(bufToString(i.name().toByteBuffer()));
				w.write("?:0x");
				w.write(ByteBufferUtil.bytesToHex(((Cell)i).value()));
			}

		} else {
			w.write(i.getClass().getSimpleName());
		}

	}

	public static String bufToString(ByteBuffer buf) {
		if ( buf == null ) return "null";
		try
		{
			return ByteBufferUtil.string(buf);
		}
		catch (CharacterCodingException e)
		{
			return "0x" + ByteBufferUtil.bytesToHex(buf);
		}
	}

	public static String printMap(Map<ByteBuffer, ByteBuffer> map)
	{
		StringWriter w = new StringWriter();
		w.append(map.getClass().getSimpleName());
		w.append('(');
		boolean first = true;
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
		{
			if (!first) w.append(',');
			first = false;
			w.append(bufToString(entry.getKey()));
			w.append('=');
			w.append(bufToString(entry.getValue()));
		}
		w.append(')');
		return w.toString();
	}

	public static String printByteBuffer(ByteBuffer buf)
	{
		StringWriter w = new StringWriter();

		byte[] data = bbToArray(buf);

		w.append('[');
		w.append(formatHex(data));


		if (data.length == 4)
		{
			w.append(" int:");
			w.append(Integer.toString(buf.duplicate().getInt()));
		}
		else if (data.length == 8)
		{
			w.append(" long:");
			w.append(Long.toString(buf.duplicate().getLong()));
		}
		w.append(']');
		return w.toString();
	}

	public static String formatHex(byte[] data)
	{
		StringWriter w = new StringWriter();
		for (byte b : data)
		{
			w.append(padLeft(Integer.toHexString(b), 2, '0'));
		}
		return w.toString();
	}

	public static String padLeft(String base, int length, char pad)
	{
		if (base.length() >= length)
		{
			return base;
		}
		return String.format("%" + (length - base.length()) + "s", "").replace(' ', pad) + base;
	}

	public static String printComposite(Composite name, ColumnFamilyStore cfs)
	{
		List<ColumnDefinition> clusters = cfs.metadata.clusteringColumns();

		StringWriter w = new StringWriter();

		w.append('[');
		for (int i = 0; i < name.size(); i++)
		{
			if (i > 0)
			{
				w.append(",");
			}

			if (clusters.size() > i) {
				w.append("CK{"+bbToString(clusters.get(i).name.bytes)+"}=");
			}
			w.append("0x"+ByteBufferUtil.bytesToHex(name.get(i)));
		}
		w.append("]");

		return w.toString();
	}

	public static byte[] bbToArray(ByteBuffer bb) {
		byte[] bytes = new byte[bb.remaining()];
		bb.duplicate().get(bytes);
		return bytes;
	}

	public static String bbToString(ByteBuffer bb) {
		return new String(bbToArray(bb));
	}

}
