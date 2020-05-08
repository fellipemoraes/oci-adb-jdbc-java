package com.oracle.fn;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;

import io.cloudevents.CloudEvent;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class MonitorarTerminaisFunction {

	private PoolDataSource poolDataSource;

	private final File walletDir = new File("/tmp", "wallet");
	private final String namespace = System.getenv().get("NAMESPACE");
	private final String bucketName = System.getenv().get("BUCKET_NAME");
	private final String dbUser = System.getenv().get("DB_USER");
	private final String dbPassword = System.getenv().get("DB_PASSWORD");
	private final String dbUrl = System.getenv().get("DB_URL");

	final static String CONN_FACTORY_CLASS_NAME = "oracle.jdbc.pool.OracleDataSource";

	private final ResourcePrincipalAuthenticationDetailsProvider provider;

	public MonitorarTerminaisFunction() {
		System.out.println("Initializing provider ...");
		provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

		System.out.println("Setting up pool data source");
		poolDataSource = PoolDataSourceFactory.getPoolDataSource();
		try {
			poolDataSource.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
			poolDataSource.setURL(dbUrl);
			poolDataSource.setUser(dbUser);
			poolDataSource.setPassword(dbPassword);
			poolDataSource.setConnectionPoolName("UCP_POOL");

		} catch (SQLException e) {
			System.out.println("Pool data source error!");
			e.printStackTrace();
		}
		System.out.println("Pool data source setup...");
	}

	@SuppressWarnings({ "rawtypes" })
	public String handleRequest(CloudEvent event) throws SQLException, JsonProcessingException {
		
		
		// String name = (input == null || input.isEmpty()) ? "world" : input;
		List<Employer> employees = this.getDataInCsv(event);
		
		String msgRetorno = this.insertDataInDB(employees);
		

	

		return msgRetorno;
	}

	@SuppressWarnings("rawtypes")
	private List<Employer> getDataInCsv(CloudEvent event) {
		List<Employer> beans = null;

		ObjectStorage client = new ObjectStorageClient(this.provider);
		client.setRegion(Region.US_ASHBURN_1);

		ObjectMapper objectMapper = new ObjectMapper();
		Map data = objectMapper.convertValue(event.getData().get(), Map.class);
		Map additionalDetails = objectMapper.convertValue(data.get("additionalDetails"), Map.class);

		GetObjectRequest objectRequest = GetObjectRequest.builder()
				.namespaceName(additionalDetails.get("namespace").toString())
				.bucketName(additionalDetails.get("bucketName").toString())
				.objectName(data.get("resourceName").toString()).build();

		GetObjectResponse objectResponse = client.getObject(objectRequest);
		try {
			client.close();

			InputStream inputStreamResponse = objectResponse.getInputStream();

			beans = new CsvToBeanBuilder<Employer>(new InputStreamReader(inputStreamResponse)).withType(Employer.class)
					.build().parse();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return beans;
	}

	@SuppressWarnings("unused")
	private List<HashMap<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();
		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		while (rs.next()) {
			HashMap<String, Object> row = new HashMap<String, Object>(columns);
			for (int i = 1; i <= columns; ++i) {
				row.put(md.getColumnName(i), rs.getObject(i));
			}
			list.add(row);
		}
		return list;
	}
	
	private String insertDataInDB(List<Employer> employees) {
		String retorno = "";
		
		System.setProperty("oracle.jdbc.fanEnabled", "false");
		
		if (needWalletDownload()) {
			System.out.println("Start wallet download...");
			downloadWallet();
			System.out.println("End wallet download!");
		}
		Connection conn;
		try {
			conn = poolDataSource.getConnection();
			conn.setAutoCommit(false);
			
			PreparedStatement stmt = conn.prepareStatement("insert into employees values(?,?,?)");
			
			for (Employer employer : employees) {
				stmt.setString(1,employer.getFirstName());
				stmt.setString(2,employer.getLastName());
				stmt.setInt(3,employer.getVisitsToWebsite());
			}
			
			int[] updateCounts = stmt.executeBatch();
			System.out.println(Arrays.toString(updateCounts));
			conn.commit();
			conn.setAutoCommit(true);
			

			conn.close();
			
			retorno = "Sucesso ==============";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retorno = "Erro ==============";
		} 
		
		return retorno;
	}

	private Boolean needWalletDownload() {
		if (walletDir.exists()) {
			System.out.println("Wallet exists, don't download it again...");
			return false;
		} else {
			System.out.println("Didn't find a wallet, let's download one...");
			walletDir.mkdirs();
			return true;
		}
	}

	private void downloadWallet() {
		// Use Resource Principal
		/*
		 * final ResourcePrincipalAuthenticationDetailsProvider provider =
		 * ResourcePrincipalAuthenticationDetailsProvider.builder().build();
		 */

		ObjectStorage client = new ObjectStorageClient(this.provider);
		client.setRegion(Region.US_ASHBURN_1);

		System.out.println("Retrieving a list of all objects in /" + namespace + "/" + bucketName + "...");
		// List all objects in wallet bucket
		ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().namespaceName(namespace)
				.bucketName(bucketName).build();
		ListObjectsResponse listObjectsResponse = client.listObjects(listObjectsRequest);
		System.out.println("List retrieved. Starting download of each object...");

		// Iterate over each wallet file, downloading it to the Function's Docker
		// container
		listObjectsResponse.getListObjects().getObjects().stream().forEach(objectSummary -> {
			System.out.println("Downloading wallet file: [" + objectSummary.getName() + "]");

			GetObjectRequest objectRequest = GetObjectRequest.builder().namespaceName(namespace).bucketName(bucketName)
					.objectName(objectSummary.getName()).build();
			GetObjectResponse objectResponse = client.getObject(objectRequest);

			try {
				File f = new File(walletDir + "/" + objectSummary.getName());
				FileUtils.copyToFile(objectResponse.getInputStream(), f);
				System.out.println("Stored wallet file: " + f.getAbsolutePath());
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

}