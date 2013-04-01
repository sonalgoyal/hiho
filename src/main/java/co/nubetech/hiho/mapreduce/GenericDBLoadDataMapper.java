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
package co.nubetech.hiho.mapreduce;

import java.io.IOException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class GenericDBLoadDataMapper<K, V> extends
		Mapper<K, V, GenericDBWritable, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.GenericDBLoadDataMapper.class);

	private ArrayList values;
	private ArrayList<ColumnInfo> tableInfo;
	private String delimiter;	

	public ArrayList<ColumnInfo> getTableInfo() {
		return tableInfo;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setTableInfo(ArrayList<ColumnInfo> tableInfo) {
		this.tableInfo = tableInfo;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		delimiter = context.getConfiguration().get(
				HIHOConf.INPUT_OUTPUT_DELIMITER);
		logger.debug("delimiter is: " + delimiter);
		String columnInfoJsonString = context.getConfiguration().get(
				HIHOConf.COLUMN_INFO);
		logger.debug("columnInfoJsonString is: " + columnInfoJsonString);
		ObjectMapper mapper = new ObjectMapper();
		tableInfo = mapper.readValue(columnInfoJsonString,
				new TypeReference<ArrayList<ColumnInfo>>() {
				});
	}

	public void map(K key, V val, Context context) throws IOException,
			InterruptedException {
		values = new ArrayList();
		
		logger.debug("Key is: " + key);
		logger.debug("Value is: " + val);
		
		StringTokenizer rowValue = new StringTokenizer(val.toString(), delimiter);
		if (rowValue.countTokens() == tableInfo.size()) {
			Iterator<ColumnInfo> iterator = tableInfo.iterator();
			while (iterator.hasNext()) {
				ColumnInfo columnInfo = iterator.next();
				String columnValue = rowValue.nextToken();
				if (columnValue == null || columnValue.trim().equals("")) {
					values.add(null);
				} else {
					logger.debug("Adding value : " + columnValue);
					int type = columnInfo.getType();
					if (type == Types.VARCHAR) {
						values.add(columnValue);
					} else if (type == Types.BIGINT) {
						values.add(Long.parseLong(columnValue));
					} else if (type == Types.INTEGER) {
						values.add(Integer.parseInt(columnValue));
					} else if (type == Types.DOUBLE) {
						values.add(Double.parseDouble(columnValue));
					} else if (type == Types.FLOAT) {
						values.add(Float.parseFloat(columnValue));
					} else if (type == Types.BOOLEAN) {
						values.add(Boolean.parseBoolean(columnValue));
					} else if (type == Types.DATE) {
						DateFormat df = new SimpleDateFormat();
						try {
							values.add(df.parse(columnValue));
						} catch (ParseException e) {
							e.printStackTrace();
							throw new IOException(e);
						}
					}
				}
			}
		} else {
			throw new IOException(
					"Number of columns specified in table is not equal to the columns contains in the file.");
		}
		GenericDBWritable gdw = new GenericDBWritable(tableInfo, values);
		context.write(gdw, null);

	}

}
