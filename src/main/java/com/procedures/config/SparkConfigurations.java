package com.procedures.config;

import java.util.Properties;

import org.apache.spark.sql.SparkSession;

public final class SparkConfigurations {
	
	private static SparkConfigurations sparkConfigurations;
	private static SparkSession sparkSession;
	
	private static String host = "jdbc:mysql://localhost:3306/warehouse1";
	private static Properties properties;
	
	public SparkConfigurations(){};

	public static SparkConfigurations getInstance(){
		if(sparkConfigurations == null){
			System.out.println("CREATED");
			sparkConfigurations = new SparkConfigurations();
			sparkSession = SparkSession.builder().appName("STORED PROCEDURES").master("local").getOrCreate();
			properties = new Properties();
			properties.put("user", "root");
			properties.put("password", "vinay123");
		}		
		return sparkConfigurations;
	}
	
	public static SparkSession createSession(){
		return sparkSession;
	}
	
	public static Properties getProps(){
		return properties;
	}
	
	public String getHost(){
		return this.host;
	}
	
}
