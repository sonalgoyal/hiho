/**
 * Copyright 2011 Nube Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package co.nubetech.hiho.pig;

import static org.junit.Assert.assertEquals;

import java.sql.Types;
import java.util.ArrayList;

import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestPigUtility {

	@Test
	public void testGetColumnType() throws HIHOException {
		assertEquals("int", PigUtility.getColumnType(Types.INTEGER));
		assertEquals("long", PigUtility.getColumnType(Types.BIGINT));
		assertEquals("float", PigUtility.getColumnType(Types.FLOAT));
		assertEquals("double", PigUtility.getColumnType(Types.DOUBLE));
		assertEquals("chararray", PigUtility.getColumnType(Types.CHAR));
		assertEquals("bytearray", PigUtility.getColumnType(Types.BINARY));
		assertEquals("bytearray", PigUtility.getColumnType(Types.BLOB));
		assertEquals("bytearray", PigUtility.getColumnType(Types.CLOB));
		try {
			PigUtility.getColumnType(Types.DATALINK);
		} catch (HIHOException h) {
			// ok
		}
	}

	@Test
	public void testGetColumns() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals("intColumn:int,stringColumn:chararray",
				PigUtility.getColumns(writable));
	}

	@Test
	public void testGetLoadScript() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals(
				"A = LOAD '/home/sgoyal/output' USING PigStorage(',') AS (intColumn:int,stringColumn:chararray);",
				PigUtility.getLoadScript("/home/sgoyal/output", writable));
	}
}
