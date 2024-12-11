package com.inissoft.collectin.activity;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inissoft.collectin.activity.process.ProcessContext;
import com.inissoft.collectin.activity.process.ProcessEngin;
import com.inissoft.collectin.component.CollectApiComponent;
import com.inissoft.collectin.component.CollectFileComponent;
import com.inissoft.collectin.component.PartitionComponent;
import com.inissoft.collectin.component.PivotingStreamComponent;
import com.inissoft.collectin.component.TransformComponent;
import com.inissoft.collectin.dto.CollectData;
import com.inissoft.collectin.dto.DataLoaderProperty;
import com.inissoft.collectin.dto.DataLoaderPropertyCollect;
import com.inissoft.collectin.dto.DataLoaderPropertyPartition;
import com.inissoft.collectin.dto.DataLoaderPropertySql;
import com.inissoft.collectin.dto.LoadData;
import com.inissoft.collectin.dto.LoadDbResult;
import com.inissoft.collectin.fileset.FileSet;
import com.inissoft.collectin.fileset.FileSetSummary;
import com.inissoft.collectin.service.DataLoaderService;
import com.inissoft.process.dto.ProcessActivity;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class CollectSftpActivity extends DefaultActivity<ProcessActivity> {
	
	public CollectSftpActivity(ProcessEngin engin, ProcessActivity activity) {
		super(engin, activity);
	}
	
	private ChannelSftp sftp;
	private Session session;
	
	@Override
	public void execute(ProcessContext context, Map<String, Object> p) {
		
		CollectFileComponent component = context.getBean("CollectFileComponent", CollectFileComponent.class);
		DataLoaderProperty property = new DataLoaderProperty();
/*
		String host = context.getPropertyString(p, "host", "");
		int port = Integer.parseInt(context.getPropertyString(p, "port", "0"));
		String username = context.getPropertyString(p, "username", "");
		String password = context.getPropertyString(p, "password", "");	
		int timeout = Integer.parseInt(context.getPropertyString(p, "timeout", "0"));
*/
		String sourceFilePath = context.getPropertyString(p, "sourceFilePath", "");
		String sourceFileName = context.getPropertyString(p, "sourceFileName", "");
		String targetFilePath = context.getPropertyString(p, "targetFilePath", "");
		
		DataLoaderPropertyCollect collect = new DataLoaderPropertyCollect();
		collect.setFilePath(sourceFilePath);
		collect.setFileName(sourceFileName);
		property.setCollect(collect);
		
		try {
			connect(p);
			downloadFile(sourceFilePath + System.lineSeparator() + sourceFileName, targetFilePath);
			
			FileSet<CollectData> CollectData = component.executeStream(property);
			p.put("CollectData", CollectData);
			context.setVariable("outputDataSet", CollectData);
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {

			disconnect();
		}

	}

	public void connect(Map<String, Object> properties) throws Exception {
		
		//String username = MapUtils.getString(properties, "username", "");
		//String password = MapUtils.getString(properties, "password", "");
		//String remoteAddr = MapUtils.getString(properties, "host", "");
		
		//int remotePort = MapUtils.getIntValue(properties, "port", 0);
		//int timeout = MapUtils.getIntValue(properties, "timeout", 0);
		
		String username = "inis";
		String password = "inis1171";
		String remoteAddr = "192.168.0.120";
		
		int remotePort = 22;
		int timeout = 3000;

		
		JSch jsch = new JSch();
		
		session = jsch.getSession(username, remoteAddr, remotePort);
		session.setPassword(password);

		// 호스트 키 검증 설정
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		//config.put("PreferredAuthentications", "password");
		session.setConfig(config);

		session.connect(timeout);
		logger.debug("Session connected");

		sftp = (ChannelSftp) session.openChannel("sftp");
		sftp.connect(timeout);
		logger.debug("SFTP channel connected");

	}
	/**
	 *  파일 다운로드
	 * @param remoteFilePath
	 * @param localFilePath
	 * @throws Exception
	 */
	public void downloadFile(String remoteFilePath, String localFilePath) throws Exception {
		if (sftp == null || !sftp.isConnected()) {
			throw new IllegalStateException("SFTP channel is not connected.");
		}
		//File sfile = new File(remoteFilePath);
		//String spath = sfile.getAbsolutePath();

		File tfile = new File(localFilePath);
		String tpath = tfile.getAbsolutePath();

		System.out.println(remoteFilePath);
		System.out.println(tpath);
		sftp.get(remoteFilePath, tpath);
		logger.debug("File downloaded: " + remoteFilePath + " -> " + localFilePath);

	}

	/**
	 *  채널 및 세션 닫기
	 */
	public void disconnect() {
		if (sftp != null && sftp.isConnected()) {
			sftp.disconnect();
			logger.debug("SFTP channel disconnected");
		}
		if (session != null && session.isConnected()) {
			session.disconnect();
			logger.debug("Session disconnected");
		}
	}


}
