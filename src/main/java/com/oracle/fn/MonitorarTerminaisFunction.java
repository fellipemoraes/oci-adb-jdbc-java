package com.oracle.fn;

import java.io.File;
import java.io.IOException;
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
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
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

	//private final ResourcePrincipalAuthenticationDetailsProvider provider;
	private final AuthenticationDetailsProvider provider;

	public MonitorarTerminaisFunction() throws IOException {
		System.out.println("Initializing provider ...");

		//provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

		String configurationFilePath = "~/.oci/config";
		String profile = "DEFAULT";

		provider = new ConfigFileAuthenticationDetailsProvider(configurationFilePath, profile);

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
		List<Employee> employees = this.getDataInCsv(event);

		String msgRetorno = this.insertDataInDB(employees);

		return msgRetorno;
	}

	@SuppressWarnings({ "rawtypes"})
	private List<Employee> getDataInCsv(CloudEvent event) {
		List<Employee> emps = new ArrayList<Employee>();

		System.out.println("Setting up client object store");
		ObjectStorage client = new ObjectStorageClient(this.provider);
		System.out.println("client object store setup...");

		client.setRegion(Region.US_ASHBURN_1);

		ObjectMapper objectMapper = new ObjectMapper();
		Map data = objectMapper.convertValue(event.getData().get(), Map.class);
		Map additionalDetails = objectMapper.convertValue(data.get("additionalDetails"), Map.class);

		GetObjectRequest objectRequest = GetObjectRequest.builder()
				.namespaceName(additionalDetails.get("namespace").toString())
				.bucketName(additionalDetails.get("bucketName").toString())
				.objectName(data.get("resourceName").toString()).build();

		GetObjectResponse objectResponse = client.getObject(objectRequest);
		
		
		CSVParser icsvParser = new CSVParserBuilder().withSeparator('|').build();
		
		//CSVReader csvReader = new CSVReader(new InputStreamReader(objectResponse.getInputStream()));
		CSVReader csvReader = new CSVReaderBuilder(new InputStreamReader(objectResponse.getInputStream())).withCSVParser(icsvParser).build();
		
		
		String[] record = null;
		
		int index = 0;
		
		try {
			while ((record = csvReader.readNext()) != null) {
				index = index + 1;
				Employee emp = new Employee();
				emp.setEmail(index + "");
				emp.setName(record[1]);
				emp.setDepartamento(record[2]);
				emps.add(emp);
			}
			csvReader.close();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(emps);
		
		
		/*try {

			InputStream inputStreamResponse = objectResponse.getInputStream();
			InputStreamReader inputStreamReader = new InputStreamReader(inputStreamResponse);

			beans = new CsvToBeanBuilder<Employee>(inputStreamReader).withType(Employee.class)
					.build().parse();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		return emps;
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

	private String insertDataInDB(List<Employee> employees) {
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

			PreparedStatement stmt = conn.prepareStatement("insert into EMPLOYEES values(?,?,?)");
			
			System.out.println("Insert data in DB...");

			for (Employee employee : employees) {
				stmt.setString(1, employee.getEmail());
				stmt.setString(2, employee.getName());
				stmt.setString(3, employee.getDepartamento());
				stmt.addBatch();
			}

			int[] updateCounts = stmt.executeBatch();
			System.out.println(Arrays.toString(updateCounts));
			conn.commit();
			
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

		@SuppressWarnings("resource")
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

}