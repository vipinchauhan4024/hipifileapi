package com.volvo.hipi.helper;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

public class AzureBlobStorageClient {

    private static  BlobServiceClient blobServiceClient;

    public static BlobServiceClient  getAzureBlobStorageClient(){
        if(blobServiceClient == null){
            System.out.println( "connecting to azure blob" );
            String connectStr = "DefaultEndpointsProtocol=https;AccountName=hipifiled3a0243711;AccountKey=s6EG457I0rcAbrKf2JHEquLsOw4S7f2LRib//f+7dqKb5XEAb9+HoDqRv0rE+ngQIFY9WSHIpB/KcQJLAjrmRw==;EndpointSuffix=core.windows.net";
             blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        }
        return blobServiceClient;
    }
}
