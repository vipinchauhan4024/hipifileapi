package com.volvo.hipi.helper;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;

public class AzureTableServiceClient {
	
	 private static TableServiceClient tableServiceClient;
	 static TableClient HipiReportBlobdataMaping;
	 private static String CONN_STR = "DefaultEndpointsProtocol=https;AccountName=hipifilesadlgen2;AccountKey=kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==;EndpointSuffix=core.windows.net"; 
     
	
	  public static TableServiceClient  getAzureTableServiceClient(){
	        if(tableServiceClient == null){
	            System.out.println( "connecting to azure table" );
	             tableServiceClient = new TableServiceClientBuilder()
	            	    .connectionString(CONN_STR)
	            	    .buildClient();
	        }
	        return tableServiceClient;
	    }
	  
	  public static TableClient getHipiReportBlobdataMapingTableClient(){
		  if(HipiReportBlobdataMaping== null){
			  HipiReportBlobdataMaping = AzureTableServiceClient.getAzureTableServiceClient().getTableClient("HipiReportBlobdataMaping");
		  }
			return HipiReportBlobdataMaping;
	  }

}
