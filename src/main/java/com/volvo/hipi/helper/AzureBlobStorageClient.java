package com.volvo.hipi.helper;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

public class AzureBlobStorageClient {

    private static  BlobServiceClient blobServiceClient;
    private static DataLakeServiceClient dataLakeServiceClient;
    private static String accountName ="hipifilesadlgen2";
    private static String accountKey ="kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==";		

    public static BlobServiceClient  getAzureBlobStorageClient(){
        if(blobServiceClient == null){
            System.out.println( "connecting to azure blob" );
           // String connectStr = "DefaultEndpointsProtocol=https;AccountName=hipifiled3a0243711;AccountKey=s6EG457I0rcAbrKf2JHEquLsOw4S7f2LRib//f+7dqKb5XEAb9+HoDqRv0rE+ngQIFY9WSHIpB/KcQJLAjrmRw==;EndpointSuffix=core.windows.net";
            String connectStr = "DefaultEndpointsProtocol=https;AccountName=hipifilesadlgen2;AccountKey=kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==;EndpointSuffix=core.windows.net"; 
            blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        }
        return blobServiceClient;
    }
    
    static public DataLakeServiceClient GetDataLakeServiceClient(){
        if (dataLakeServiceClient != null){
        	return dataLakeServiceClient;
        }
    	
        StorageSharedKeyCredential sharedKeyCredential =
            new StorageSharedKeyCredential(accountName, accountKey);

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        builder.credential(sharedKeyCredential);
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");
        dataLakeServiceClient = builder.buildClient();
        return dataLakeServiceClient;
    }
}
