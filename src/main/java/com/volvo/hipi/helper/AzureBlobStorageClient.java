package com.volvo.hipi.helper;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

public class AzureBlobStorageClient {

	private static BlobServiceClient blobServiceClient;
	private static DataLakeServiceClient dataLakeServiceClient;
	private static BlobContainerClient bclient;
	private static String accountName ="hipifilesadlgen2";
	// private static String accountKey
	// ="kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==";
	//private static String accountName = "hipideveuwst01";
	private static String accountKey = "2J91Z3i5eQK8uURipAnKLo4PbyKthtb/+SV+aD9VyQk352WXL/EVMT+d3tqrvPnkUGJlyoU2nvATUKp2Gz3CnQ==";
	//private static String connectStr = "DefaultEndpointsProtocol=https;AccountName=hipideveuwst01;AccountKey=2J91Z3i5eQK8uURipAnKLo4PbyKthtb/+SV+aD9VyQk352WXL/EVMT+d3tqrvPnkUGJlyoU2nvATUKp2Gz3CnQ==;EndpointSuffix=core.windows.net";
	private static String connectStr="DefaultEndpointsProtocol=https;AccountName=hipifilesadlgen2;AccountKey=kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==;EndpointSuffix=core.windows.net";
	public static BlobServiceClient getAzureBlobStorageClient() {
		if (blobServiceClient == null) {
			System.out.println("connecting to azure blob");
			// String connectStr =
			// "DefaultEndpointsProtocol=https;AccountName=hipifiled3a0243711;AccountKey=s6EG457I0rcAbrKf2JHEquLsOw4S7f2LRib//f+7dqKb5XEAb9+HoDqRv0rE+ngQIFY9WSHIpB/KcQJLAjrmRw==;EndpointSuffix=core.windows.net";
			blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
		}
		return blobServiceClient;
	}

	public static BlobContainerClient getAzureBlobContainerClient(String containername) {
		if (bclient != null) {
			return bclient;
		}
		bclient = new BlobContainerClientBuilder().connectionString(connectStr).containerName(containername)
				.buildClient();
		return bclient;
	}
	
/*	https
 * static public DataLakeServiceClient GetDataLakeServiceClient() {
		if (dataLakeServiceClient != null) {
			return dataLakeServiceClient;
		}

		StorageSharedKeyCredential sharedKeyCredential =
				// new StorageSharedKeyCredential(accountName, accountKey);

				StorageSharedKeyCredential.fromConnectionString(connectStr);
		DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

		builder.credential(sharedKeyCredential);
		builder.endpoint("https://" + accountName + ".dfs.core.windows.net");
		dataLakeServiceClient = builder.buildClient();
		return dataLakeServiceClient;
	}*/

	static public DataLakeServiceClient GetDataLakeServiceClient() {
		if (dataLakeServiceClient != null) {
			return dataLakeServiceClient;
		}

		StorageSharedKeyCredential sharedKeyCredential =
				// new StorageSharedKeyCredential(accountName, accountKey);

				StorageSharedKeyCredential.fromConnectionString(connectStr);
		DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

		builder.credential(sharedKeyCredential);
		builder.endpoint("http://" + accountName + ".dfs.core.windows.net");
		dataLakeServiceClient = builder.buildClient();
		return dataLakeServiceClient;
	}
}
