package com.inissoft.collectin.activity;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.MapUtils;

import com.inissoft.collectin.activity.process.ProcessContext;
import com.inissoft.collectin.activity.process.ProcessEngin;
import com.inissoft.collectin.component.CollectFileComponent;
import com.inissoft.collectin.dto.CollectData;
import com.inissoft.collectin.dto.DataLoaderProperty;
import com.inissoft.collectin.dto.DataLoaderPropertyCollect;
import com.inissoft.collectin.fileset.FileSet;
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

		try {
			FileSet<CollectData> collectFileSet = null;

			DataLoaderProperty property = new DataLoaderProperty();

			String sourceFilePath = context.getPropertyString(p, "sourceFilePath", "");
			String sourceFileName = context.getPropertyString(p, "sourceFileName", "");
			String targetFilePath = context.getPropertyString(p, "targetFilePath", "");

			DataLoaderPropertyCollect collect = new DataLoaderPropertyCollect();
			collect.setFilePath(targetFilePath);
			collect.setFileName(sourceFileName);
			property.setCollect(collect);

			connect(p);
			downloadFile(sourceFilePath + sourceFileName, targetFilePath + File.separator);
			disconnect();

			context.setVariable("inputDataSet", collectFileSet);
			p.put("inputDataSet", collectFileSet);

		} catch (Exception e) {
			logger.error("ERROR:" + e.getMessage());
			e.printStackTrace();
		}

	}

	public void connect(Map<String, Object> properties) throws Exception {

		String username = MapUtils.getString(properties, "username", "");
		String password = MapUtils.getString(properties, "password", "");
		String remoteAddr = MapUtils.getString(properties, "host", "");

		int remotePort = MapUtils.getIntValue(properties, "port", 0);
		int timeout = MapUtils.getIntValue(properties, "timeout", 0);

		JSch jsch = new JSch();
		session = jsch.getSession(username, remoteAddr, remotePort);
		session.setPassword(password);

		// 호스트 키 검증 설정
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
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
			throw new IllegalStateException("SFTP channel is not connected");
		}

		File tfile = new File(localFilePath);
		String tpath = tfile.getAbsolutePath();

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
	
	private void makePath() {}


}
