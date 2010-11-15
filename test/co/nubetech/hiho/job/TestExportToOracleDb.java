package co.nubetech.hiho.job;

import static org.junit.Assert.*;
import junit.framework.*;


import org.junit.*;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;


public class TestExportToOracleDb {
	
	@Test
	public void testTableCorrect() throws HIHOException {
		String query = "create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_diraccess parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
	    assertEquals("age", ExportToOracleDb.getTableName(query));
	    
	}
	
	@Test
	public void testTableWithSpaceCorrect() throws HIHOException {
		String query = "create table age (  i   Number,  n   Varchar(20),  a   Number)organization external (  type oracle_loader  default directory ext_diraccess parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
	    assertEquals("age", ExportToOracleDb.getTableName(query));
	    
	}
	
	@Test(expected=HIHOException.class)
	public void testTableNoBrackets() throws HIHOException {
		String query = "create age ";
	    ExportToOracleDb.getTableName(query);
	    
	}
}	
	
