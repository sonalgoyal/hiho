/**
 * Copyright 2010 Nube Technologies
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

import org.apache.log4j.Logger;

public class ColumnInfo {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.ColumnInfo.class);

	private int index;
	private int type;
	private String name;

	public ColumnInfo(int index, int type, String name) {
		this.index = index;
		this.type = type;
		this.name = name;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String toString() {
		return "Column " + this.index + ", name " + this.name + ", type "
				+ this.type;
	}

}
