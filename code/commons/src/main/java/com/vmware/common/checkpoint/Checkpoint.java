package com.vmware.common.checkpoint;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.conf.AppConfig;

public class Checkpoint {

	private static File file = null;
	private static Properties properties = null;
	private static final Logger logger = LogManager.getLogger(Checkpoint.class);
	
	public static void init() throws Exception {

		if(properties == null){
			properties = new Properties();
			if(AppConfig.checkpointFile == null || AppConfig.checkpointFile.isEmpty()){
				file = new File("dim_checkpoint.chkpt");
			}else{
				file = new File(AppConfig.checkpointFile);
			}			
		}
		
		file.createNewFile();		
		try(FileInputStream fileInputStream = new FileInputStream(file)){
			properties.load(fileInputStream);
		}catch (Exception e) {
			logger.error("Error reading the checkpoint file");
		}

	}
	
	public static void update(String key, Set<String> connectionNames) throws Exception{
		
		try(OutputStream outputStream = new FileOutputStream(file)){
			String value = "";
			String delimiter = "";
			for(String connName : connectionNames){
				value = value + delimiter + connName;
				delimiter = ",";
			}
			properties.setProperty(key, value);
			properties.store(outputStream, System.currentTimeMillis()+"");	
		}
		
	}
	
	public static String getProperty(String key){
		return properties.getProperty(key,null);
	}
	
}
