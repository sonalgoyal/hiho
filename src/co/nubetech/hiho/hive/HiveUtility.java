package co.nubetech.hiho.hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.OutputStrategyEnum;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class HiveUtility {
	
	public static void createTable(Configuration conf, Job job, GenericDBWritable writable) 
		throws HIHOException {
		try {
			File file = new File(new File(conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH)), "hiveScript" + job.getJobID() + ".txt");
			FileOutputStream fos = new FileOutputStream(file);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fos));
			w.write(getCreateScript(conf, writable));
			w.close();
			fos.close();
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new HIHOException("Could not generate hive table", e );
		}
	}
	
	/**
	 * Returns the load script for a hdfs folder
	 * and a jdbc query result set metadata
	 * @param hdfsDir
	 * @param writable
	 * @return
	 */
	public static String getCreateScript(Configuration conf, GenericDBWritable writable) throws HIHOException{
		StringBuilder builder = new StringBuilder();
		String tableName = getTableName();
		builder.append("CREATE TABLE " + tableName);
		builder.append(getColumns(writable));
		builder.append("ROW FORMAT ");
		OutputStrategyEnum outputStrategy = OutputStrategyEnum.valueOf(conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY));
		if (outputStrategy == OutputStrategyEnum.DELIMITED) {
			builder.append("DELIMITED FIELDS TERMINATED BY ");
			builder.append(conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER));
			builder.append("LINES TERMINATED BY \n STORED AS TEXTFILE");
		}
		builder.append("LOAD DATA INPATH '");
		//builder.append(hdfsDir);
		builder.append("') OVERWRITE INTO TABLE ");
		builder.append(tableName);
		return builder.toString();
	}
	
	/**
	 * Returns the load script for a hdfs folder
	 * and a jdbc query result set metadata
	 * @param hdfsDir
	 * @param writable
	 * @return
	 */
	public static String getCreateAndLoadScript(String hdfsDir, GenericDBWritable writable) throws HIHOException{
		StringBuilder builder = new StringBuilder();
		String tableName = getTableName();
		builder.append("CREATE TABLE " + tableName);
		builder.append(getColumns(writable));
		builder.append("ROW FORMAT ");
		//if ()
		builder.append("LOAD DATA INPATH '");
		builder.append(hdfsDir);
		builder.append("') OVERWRITE INTO TABLE ");
		builder.append(tableName);
		return builder.toString();
	}
	
	public static String getTableName() {
		return null;
	}
	
	public static String getColumns(GenericDBWritable writable) throws HIHOException{
		StringBuilder builder = new StringBuilder();
		ArrayList<ColumnInfo> columns = writable.getColumns();
		for (ColumnInfo column: columns) {
			builder.append(column.getName());
			builder.append(":");
			builder.append(getColumnType(column.getType()));
			builder.append(",");
		}
		//above loop adds extra comma, removing.
		if (builder.length() > 0) {
			builder.deleteCharAt(builder.lastIndexOf(","));
		}
		return builder.toString();		
	}
	
	public static String getColumnType(int columnType) throws HIHOException{
		String returnType = null;
		switch (columnType) {
		
		case Types.BIGINT:
			returnType = "long";
			break;
		case Types.BOOLEAN:
		case Types.CHAR:
			returnType = "chararray";
			break;
		case Types.DECIMAL:
		case Types.REAL:
		case Types.NUMERIC:
		case Types.DOUBLE:
			returnType = "double";
			break;
		case Types.FLOAT:
			returnType = "float";
			break;
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			returnType = "int";
			break;
		case Types.BINARY:
		case Types.CLOB:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			returnType = "bytearray";
			break;
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			returnType = "chararray";
			break;
		default:
			throw new HIHOException("Unsupported type");
		}
		return returnType;
	}

}
