package co.nubetech.hiho.pig;

import static org.junit.Assert.*;

import java.sql.Types;
import java.util.ArrayList;

import org.junit.*;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestPigUtility {
	
	@Test
	public void testGetColumnType() throws HIHOException{
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
		}
		catch(HIHOException h) {
			//ok
		}
	}
	
	@Test
	public void testGetColumns() throws HIHOException{
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR, "stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals("intColumn:int,stringColumn:chararray", PigUtility.getColumns(writable));
	}
	
	@Test
	public void testGetLoadScript() throws HIHOException{
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR, "stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals("A = LOAD ('/home/sgoyal/output') as (intColumn:int,stringColumn:chararray);", 
				PigUtility.getLoadScript("/home/sgoyal/output", writable));
	}
}
