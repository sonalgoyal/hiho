package co.nubetech.hiho.mapreduce;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Matchers.anyObject;

import java.io.FileInputStream;
import java.io.StringBufferInputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;

public class TestOracleLoadMapper {

	@Test
	public final void testSetup() throws Exception {
		Mapper.Context context = mock(Mapper.Context.class);
		OracleLoadMapper mapper = new OracleLoadMapper();
		FTPClient ftpClient = mock(FTPClient.class);
		Configuration conf = new Configuration();
		String ip = "192.168.128.8";
		String portno = "21";
		String user = "nube";
		String password = "nube123";
		String externalDirectory = "dir";
		conf.set(HIHOConf.ORACLE_FTP_ADDRESS, ip);
		conf.set(HIHOConf.ORACLE_FTP_PORT, portno);
		conf.set(HIHOConf.ORACLE_FTP_USER, user);
		conf.set(HIHOConf.ORACLE_FTP_PASSWORD, password);
		conf.set(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR, externalDirectory);
		when(context.getConfiguration()).thenReturn(conf);
		mapper.setFtpClient(ftpClient);
		mapper.setup(context);
		verify(ftpClient).connect(ip, Integer.parseInt(portno));
		verify(ftpClient).login(user, password);
		verify(ftpClient).changeWorkingDirectory(externalDirectory);
	}

	@Test
	public final void testMapper() throws Exception {
		Mapper.Context context = mock(Mapper.Context.class);
		OracleLoadMapper mapper = new OracleLoadMapper();
		FTPClient ftpClient = mock(FTPClient.class);
		FSDataInputStream val=mock(FSDataInputStream.class);
		Text key = new Text("key");
		mapper.setFtpClient(ftpClient);
		mapper.map(key, val, context);
		verify(ftpClient).appendFile("key", val);
	}

}
