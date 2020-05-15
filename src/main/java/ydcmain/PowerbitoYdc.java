package ydcmain;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;



import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.UpsertResult;
import com.sforce.soap.enterprise.sobject.BI__c;
import com.sforce.soap.enterprise.sobject.Data_Source__c;
import com.sforce.soap.enterprise.sobject.PowerBI_Dashboard__c;
import com.sforce.soap.enterprise.sobject.PowerBI_Report__c;
import com.sforce.soap.enterprise.sobject.PowerBI_Tile__c;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import ydc.powerbi.client.Dashboard;
import ydc.powerbi.client.DataSet;
import ydc.powerbi.client.DataSources;
import ydc.powerbi.client.PowerBI;
import ydc.powerbi.client.Report;
import ydc.powerbi.client.Tiles;





public class PowerbitoYdc {
	final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	static PowerBI powerBI;
	static EnterpriseConnection connection;
	static List<String> dashboardId = new ArrayList<String>();
	static List<String> datasetids = new ArrayList<String>();
	static String biserverid;
	static Properties prop = new Properties(); 
	
	
	public static void main(String[] args) throws ConnectionException, FileNotFoundException, IOException {
		InputStream input = new FileInputStream("src/main/java/config.properties");
		prop.load(input);
	powerBI = new PowerBI(prop.getProperty("baseurl"), prop.getProperty("powerbiusername"), prop.getProperty("powerbipassword"),
	prop.getProperty("clinetid"),prop.getProperty("tenantid"),prop.getProperty("powerbiloginurl"),prop.getProperty("driver"));
	String token = powerBI.getToken();
	 ConnectorConfig config = new ConnectorConfig();
	 config.setUsername(prop.getProperty("Salesforceusername"));
	 config.setPassword(prop.getProperty("Salesforcepassword"));
	  connection = Connector.newConnection(config);
	  PowerbitoYdc ydc2pbi = new PowerbitoYdc();
	  ydc2pbi.getBiserver(connection);
	  ydc2pbi.getDashboards(token,connection,biserverid);
	  ydc2pbi.getDatasets(token, connection);
	  ydc2pbi.getReports(token, connection);
	  ydc2pbi.getTiles(token, connection);
	  //a.getTables(token, connection);
	}
	public void getBiserver(EnterpriseConnection connection) throws ConnectionException {
		BI__c bi = new BI__c();
		bi.setUsername__c(prop.getProperty("powerbiusername"));
		bi.setPassword__c(prop.getProperty("powerbipassword"));
		bi.setSite__c(prop.getProperty("baseurl"));
		bi.setURL__c(prop.getProperty("powerbiloginurl"));
		bi.setName("powerBi");
		SObject[] upserts = new BI__c[1];
		upserts[0]=bi;
		UpsertResult[] upsertResults = connection.upsert( "Name", upserts);
		for(UpsertResult result:upsertResults) {
			if(result.isSuccess()) {
			logger.log(Level.INFO, "Succesfully upserted Bi server");
			biserverid = result.getId();
			}
			else {
			logger.log(Level.SEVERE, "Failed to upsert Bi server due to following error :-> "+result.getErrors()[0].getMessage());
			}
			}

		
	}
	public  void getDashboards(String token,EnterpriseConnection connection,String Biserver) throws ConnectionException {
	List<Dashboard> dashboards = powerBI.getPowerBIDashboards(token);
	PowerBI_Dashboard__c sfdashboard = new PowerBI_Dashboard__c();
	for(Dashboard dashboard:dashboards) {
		dashboardId.add(dashboard.getId());
	SObject[] upserts = new PowerBI_Dashboard__c[1];
	sfdashboard.setName(dashboard.getDisplayName());
	sfdashboard.setExternal_Id__c(dashboard.getId());
	sfdashboard.setExternal_URL__c(dashboard.getEmbedUrl());
	sfdashboard.setBI_Server__c(Biserver);
	upserts[0]=sfdashboard;
	UpsertResult[] upsertResults = connection.upsert( "External_Id__c", upserts);

	for(UpsertResult result:upsertResults) {
	if(result.isSuccess()) {
	logger.log(Level.INFO, "Succesfully upserted Dashboard");
	}
	else {
	logger.log(Level.SEVERE, "Failed to upsert Dashboard due to following error :-> "+result.getErrors()[0].getMessage());
	}
	}

	}
	}
	public void getTiles(String token,EnterpriseConnection connection) throws ConnectionException {
	PowerBI_Tile__c sftile = new PowerBI_Tile__c();
	for(String did:dashboardId) {
	PowerBI_Dashboard__c sfdashboard = new PowerBI_Dashboard__c();
	sfdashboard.setExternal_Id__c(did);
	List<Tiles> tiles = powerBI.getPowerBITiles(token, did);
	for(Tiles tile:tiles) {
	SObject[] upserts = new PowerBI_Tile__c[1];
	if(tile.getReportId() != null) {
	QueryResult rep = connection.query("Select Id from PowerBI_Report__c where External_Id__c ='"+tile.getReportId()+"'");
	for(int i=0;i<rep.getSize();i++) {
	PowerBI_Report__c t = (PowerBI_Report__c)rep.getRecords()[i];
	sftile.setPowerBI_Report__c(t.getId());
	    }  
	}
	else {logger.log(Level.INFO, "No report found");
	sftile.setPowerBI_Report__c(null);}
	sftile.setName(tile.getTitle());
	sftile.setExternal_Id__c(tile.getId());
	sftile.setExternal_URL__c(tile.getEmbedUrl());
	sftile.setPowerBI_Dashboard__r(sfdashboard);
	sftile.setDescription__c(tile.getSubTitle());
	upserts[0]=sftile;
	UpsertResult[] upsertResults = connection.upsert( "External_Id__c", upserts);
	for(UpsertResult result:upsertResults) {
	if(result.isSuccess()) {
		logger.log(Level.INFO, "Succesfully upserted Tile");
	}
	else {
		logger.log(Level.SEVERE, "Failed to upsert Tile due to following error :-> "+result.getErrors()[0].getMessage());
	}
	}

	}
	}

	}
	public void getReports(String token,EnterpriseConnection connection) throws ConnectionException {
	PowerBI_Report__c sfreport = new PowerBI_Report__c();
	List<Report> reports = powerBI.getPowerBIReports(token);
	for(Report report:reports) {
	SObject[] upserts = new PowerBI_Report__c[1];
	QueryResult datasetquery = connection.query("select Id from Data_Source__c where External_Id__c='"+report.getDatasetId()+"'");
	for(int i=0;i<datasetquery.getSize();i++) {
		Data_Source__c datasetExternalid = (Data_Source__c)datasetquery.getRecords()[i];
	sfreport.setData_Source__c(datasetExternalid.getId());
	    }  
	sfreport.setName(report.getName());
	sfreport.setExternal_Id__c(report.getId());
	sfreport.setExternal_URL__c(report.getEmbedUrl());


	upserts[0]=sfreport;
	UpsertResult[] upsertResults = connection.upsert( "External_Id__c", upserts);
	for(UpsertResult result:upsertResults) {
	if(result.isSuccess()) {
		logger.log(Level.INFO, "Succesfully upserted Report");
	}
	else {
		logger.log(Level.SEVERE, "Failed to upsert Report due to following error :-> "+result.getErrors()[0].getMessage());
	}
	}

	}
	}
	
	public void getDatasets(String token,EnterpriseConnection connection) throws ConnectionException {
		Data_Source__c sfdatasource = new Data_Source__c();
	List<DataSet> datasets = powerBI.getPowerBIDataSets(token);
	for(DataSet dataset:datasets) {
	SObject[] upserts = new Data_Source__c[1];
	sfdatasource.setName(dataset.getName());
	sfdatasource.setExternal_Id__c(dataset.getId());
	
	List<DataSources> datasources = powerBI.getdatasource(token, dataset.getId());
	System.out.println(datasources);
	if(datasources.isEmpty()) {
		sfdatasource.setType__c("PowerBI");
		System.out.println("null");
	}else {
	for(DataSources datasource:datasources) {

		if(datasource != null && datasource.getDatasourceType().equals("Sql")) {	
			sfdatasource.setType__c("SQL");
			System.out.println("SQL");
			break;
		}else if(datasource != null && datasource.getDatasourceType().equals("Extension") && datasource.getConnectionDetails().getKind().equals("Snowflake")){
			sfdatasource.setType__c("Snowflake");
			System.out.println("Snowflake");
			break;
		}
		else if(datasource != null) {
			sfdatasource.setType__c("PowerBI");
			System.out.println("not null");
			break;
			}
			}
	}
	datasetids.add(dataset.getId());
	upserts[0]=sfdatasource;
	UpsertResult[] upsertResults = connection.upsert( "External_Id__c", upserts);
	for(UpsertResult result:upsertResults) {
	if(result.isSuccess()) {
		logger.log(Level.INFO, "Succesfully upserted Dataset");
	}
	else {
		logger.log(Level.SEVERE, "Failed to upsert Dataset due to following error :-> "+result.getErrors()[0].getMessage());
	}
	} 
	} 

	}



}
