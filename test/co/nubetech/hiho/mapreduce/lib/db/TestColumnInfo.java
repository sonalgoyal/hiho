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
package co.nubetech.hiho.mapreduce.lib.db;

import static org.junit.Assert.assertEquals;

import java.sql.Types;

import org.junit.Test;

public class TestColumnInfo {

	@Test
	public void testGetIndex() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		assertEquals(1, cInfo.getIndex());
	}

	@Test
	public void testSetIndex() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		cInfo.setIndex(2);
		assertEquals(2, cInfo.getIndex());
	}

	@Test
	public void testGetType() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		assertEquals(Types.INTEGER, cInfo.getType());

	}

	@Test
	public void testSetType() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		cInfo.setType(Types.VARCHAR);
		assertEquals(Types.VARCHAR, cInfo.getType());

	}

	@Test
	public void testGetName() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		assertEquals("name", cInfo.getName());

	}

	@Test
	public void testSetName() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		cInfo.setName("Student");
		assertEquals("Student", cInfo.getName());

	}

	@Test
	public void testToString() {
		ColumnInfo cInfo = new ColumnInfo(1, Types.INTEGER, "name");
		assertEquals("Column 1, name name, type 4", cInfo.toString());

	}
}
