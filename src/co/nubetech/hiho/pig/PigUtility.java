package co.nubetech.hiho.pig;

import java.sql.Types;
import java.util.ArrayList;

import org.apache.avro.Schema;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

/**
 * Generic bunch of utilities which can be used to create Pig schema
 * @author sgoyal
 *
 */
public class PigUtility {
	
	/**
	 * Returns the load script for a hdfs folder
	 * and a jdbc query result set metadata
	 * @param hdfsDir
	 * @param writable
	 * @return
	 */
	public static String getLoadScript(String hdfsDir, GenericDBWritable writable) throws HIHOException{
		StringBuilder builder = new StringBuilder();
		builder.append("A = LOAD ('");
		builder.append(hdfsDir);
		builder.append("') as (");
		builder.append(getColumns(writable));
		builder.append(");");
		return builder.toString();
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
