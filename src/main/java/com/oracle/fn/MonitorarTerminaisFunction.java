package com.oracle.fn;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
//	private final AuthenticationDetailsProvider provider;

	public MonitorarTerminaisFunction() throws IOException {
		System.out.println("Initializing provider ...");

		provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

//		String configurationFilePath = "~/.oci/config";
//		String profile = "DEFAULT";
//
//		provider = new ConfigFileAuthenticationDetailsProvider(configurationFilePath, profile);

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
		List<Terminal> terminais = this.getDataInCsv(event);

		String msgRetorno = this.insertDataInDB(terminais);

		return msgRetorno;
	}

	@SuppressWarnings({ "rawtypes" })
	private List<Terminal> getDataInCsv(CloudEvent event) {
		List<Terminal> terminais = new ArrayList<Terminal>();

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

		// CSVReader csvReader = new CSVReader(new
		// InputStreamReader(objectResponse.getInputStream()));
		CSVReader csvReader = new CSVReaderBuilder(new InputStreamReader(objectResponse.getInputStream()))
				.withCSVParser(icsvParser).build();

		String[] record = null;

		try {
			while ((record = csvReader.readNext()) != null) {
				terminais.add(this.convertCsvToTerminal(record));
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

		return terminais;
	}

	private Terminal convertCsvToTerminal(String record[]) {
		Terminal terminal = new Terminal();
		terminal.setColuna1(record[0]);
		terminal.setColuna2(record[1]);
		terminal.setColuna3(record[2]);
		terminal.setColuna4(record[3]);
		terminal.setColuna5(record[4]);
		terminal.setColuna6(record[5]);
		terminal.setColuna7(record[6]);
		terminal.setColuna8(record[7]);
		terminal.setColuna9(record[8]);
		terminal.setColuna10(record[9]);
		terminal.setColuna11(record[10]);
		terminal.setColuna12(record[11]);
		terminal.setColuna13(record[12]);
		terminal.setColuna14(record[13]);
		terminal.setColuna15(record[14]);
		terminal.setColuna16(record[15]);
		terminal.setColuna17(record[16]);
		terminal.setColuna18(record[17]);

		return terminal;
	}

	private String insertDataInDB(List<Terminal> terminais) {
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

			Statement stmtSeq = conn.createStatement();

			conn.setAutoCommit(false);

			PreparedStatement stmt = conn.prepareStatement("insert into TERMINAIS values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

			System.out.println("Inserting data in DB...");

			for (Terminal terminal : terminais) {
				ResultSet rs = stmtSeq.executeQuery("SELECT terminais_seq.NEXTVAL FROM dual");
				int seq_id = 0;

				if (rs != null && rs.next()) {
					seq_id = rs.getInt(1);

					rs.close();
				}

				stmt.setInt(1, seq_id);
				stmt.setString(2, terminal.getColuna1());
				stmt.setString(3, terminal.getColuna2());
				stmt.setString(4, terminal.getColuna3());
				stmt.setString(5, terminal.getColuna4());
				stmt.setString(6, terminal.getColuna5());
				stmt.setString(7, terminal.getColuna6());
				stmt.setString(8, terminal.getColuna7());
				stmt.setString(9, terminal.getColuna8());
				stmt.setString(10, terminal.getColuna9());
				stmt.setString(11, terminal.getColuna10());
				stmt.setString(12, terminal.getColuna11());
				stmt.setString(13, terminal.getColuna12());
				stmt.setString(14, terminal.getColuna13());
				stmt.setString(15, terminal.getColuna14());
				stmt.setString(16, terminal.getColuna15());
				stmt.setString(17, terminal.getColuna16());
				stmt.setString(18, terminal.getColuna17());
				stmt.setString(19, terminal.getColuna18());

				stmt.addBatch();
			}

			stmt.executeBatch();
			//int[] updateCounts = stmt.executeBatch();
			//System.out.println(Arrays.toString(updateCounts));

			stmtSeq.close();

			conn.commit();

			conn.close();

			System.out.println("Data sucessful inserted in DB...");

			retorno = "Data from TERMINAIS sucessful inserted in DB...";
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retorno = "Error to insert Data from TERMINAIS in DB...";
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