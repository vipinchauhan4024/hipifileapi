package com.volvo.hipi.azureservices;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.volvo.hipi.helper.AzureBlobStorageClient;
import com.volvo.hipi.helper.AzureTableServiceClient;

@Service
public class DownloadFromAzure {
	
	Logger logger = LoggerFactory.getLogger(DownloadFromAzure.class);
	
	public Set<String> getBlobIdsForReportIdNum(String reportNo,String reportId,String reportType) throws Exception{
		logger.info("getting blobids files for reportId : "+reportId);
		TableClient tableClient = AzureTableServiceClient.getHipiReportBlobdataMapingTableClient();
		if(tableClient == null){
			throw new Exception("Table not found");
		}
		try{
		TableEntity te = tableClient.getEntity(reportNo, reportId);
		if (te != null) {
			String blobids = (String) te.getProperty("blobdataid");
			if (blobids != null && !"".equals(blobids)) {
				return new HashSet<>(Arrays.asList(blobids.split(",")));
			}
		}
		} catch(TableServiceException e){
			logger.error(e.getMessage());
		}
	    return new HashSet<>();
	}

	public List<File> getAttachmentsFromAzureBlob(String reportNo, String reportId, String reportType) throws Exception {
		logger.info("Downloading files for report : "+reportNo);
		if(reportId == null ||reportId== null || reportType== null){
			throw new Exception("reportnumber , reportid and reportType required ");
		}
		Set<String> reportIdAttachmentBlobIds = new HashSet<>();//getBlobIdsForReportIdNum(reportNo,reportId,reportType);
		Date d = new Date(System.currentTimeMillis());
		connectToUrl("https://portal.azure.com");
		connectToUrl("http://portal.azure.com/hipifilesadlgen2.dfs.core.windows.net");
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();



		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("protusfiles");//("hipi");
		System.out.println("file system "+dataLakeFileSystemClient.getFileSystemUrl());
		List<File> files = new ArrayList<>();


		//DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient("Attachments").getSubdirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);
		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);


		System.out.println(directoryClient.getDirectoryUrl());
		logger.info("directoryClient: "+directoryClient.getDirectoryUrl());
		DataLakeFileClient fileClient ;
		File file;
		PagedIterable<PathItem> pagedIterable = directoryClient.listPaths();//directoryClient.listPaths(false, false, 100, java.time.Duration.ofMillis(100000l));
		logger.info("pagedIterable 1: "+pagedIterable);
		java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
		logger.info("PathItem: "+iterator);
		String filename;
		PathItem item; 
		List<String> filesPaths = new ArrayList<>();
		try{
			while (iterator.hasNext()) {
				item = iterator.next();
				if (item!=null && !item.isDirectory()) {
					System.out.println(item.getName());
					logger.info("filename from azure "+item.getName());
					filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
					if (matchingBlobId(reportIdAttachmentBlobIds, filename)) {
						System.out.println(filename);
						filesPaths.add(item.getName());
						logger.info("filename from azure "+item.getName());
						/*fileClient = directoryClient.getFileClient(filename);
						file = new File(timestamp + filename);
						fileClient.readToFile(timestamp + filename, true);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);*/
					}

				} else if(reportId != null && !"".equals(reportId)) {
					String  dirName= item.getName().substring(item.getName().lastIndexOf("/") + 1);
					System.out.println(dirName );
					logger.info(dirName);
					addReportFilesFromSubDir(reportId, dirName, directoryClient, timestamp,files, filesPaths);
				}
			}

		} catch(Exception e){
			logger.info("Exception-azure : "+ e.getMessage());
			throw e;
		}
		
		files =getDownloadUsingAzureBlobStorageClient(filesPaths);
		return files;
	}
	
	/*public List<File> getAttachmentsFromAzureBlob(String reportNo, String reportId, String reportType) throws Exception {
		logger.info("Downloading files for report : "+reportNo);
		if(reportId == null ||reportId== null || reportType== null){
			throw new Exception("reportnumber , reportid and reportType required ");
		}
		Set<String> reportIdAttachmentBlobIds = new HashSet<>();//getBlobIdsForReportIdNum(reportNo,reportId,reportType);
		Date d = new Date(System.currentTimeMillis());
		//connectToUrl("https://portal.azure.com");
		//connectToUrl("https://portal.azure.com/hipifilesadlgen2.dfs.core.windows.net");
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();
		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("hipi");
		List<File> files = new ArrayList<>();
		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient("Attachments").getSubdirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);
		System.out.println(directoryClient.getDirectoryUrl());
		
		DataLakeFileClient fileClient ;//= directoryClient.getFileClient("pr_mig_1092286_DQ6376M.pdf");
		File file; //=  new File(timestamp + "pr_mig_1092286_DQ6376M.pdf");
			
		copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
		files.add(file);
		
		PagedIterable<PathItem> pagedIterable = directoryClient.listPaths(false, false, 100, java.time.Duration.ofMillis(100000l));
		java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
		String filename;
		PathItem item;
		try{
		while (iterator.hasNext()) {
			  item = iterator.next();
				if (item!=null && !item.isDirectory()) {
                    
					System.out.println(item.getName());
					filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
					if (matchingBlobId(reportIdAttachmentBlobIds, filename)) {
						System.out.println(filename);
						fileClient = directoryClient.getFileClient(filename);
						file = new File(timestamp + filename);
						
						OutputStream targetStream = new FileOutputStream(file);
						fileClient.read(targetStream);
						targetStream.close();
						fileClient.flush(file.length());
						fileClient.readToFile(timestamp + filename, true);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);
					}
					
				} else if(reportId != null && !"".equals(reportId)) {
					 String  dirName= item.getName().substring(item.getName().lastIndexOf("/") + 1);
				     System.out.println(dirName );
				     addReportFilesFromSubDir(reportId, dirName, directoryClient, timestamp,files);
				}
		}

		} catch(Exception e){
			e.printStackTrace();
		}
		return files;
	}
	*/
	public void listFilesInDirectory(DataLakeFileSystemClient fileSystemClient, String path){
	    
	    ListPathsOptions options = new ListPathsOptions();
	    options.setPath(path);
	 
	    PagedIterable<PathItem> pagedIterable = 
	    fileSystemClient.listPaths(options, null);

	    java.util.Iterator<PathItem> iterator = pagedIterable.iterator();

	   
	    PathItem item = iterator.next();

	    while (item != null)
	    {
	        System.out.println(item.getName());


	        if (!iterator.hasNext())
	        {
	            break;
	        }
	        
	        item = iterator.next();
	    }

	}
	

	 private void addReportFilesFromSubDir(String reportId, String dirName, DataLakeDirectoryClient directoryClient, String timestamp, List<File> files, List<String> filesPaths ) {
		 DataLakeDirectoryClient client=directoryClient.getSubdirectoryClient(dirName).getSubdirectoryClient(reportId);
		 if(client != null){
			 logger.info("loading "+ dirName +"  for reportid  "+ reportId);
			 PagedIterable<PathItem> pagedIterable = client.listPaths();
			 java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
			String filename;
			File file;
			PathItem item;
			DataLakeFileClient fileClient;
			try{
			 while (iterator.hasNext()) {
				  item = iterator.next();
					if (item!=null && !item.isDirectory()) {
						logger.info(item.getName());
						filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
						logger.info(filename);
						filesPaths.add(item.getName());
						/*fileClient = client.getFileClient(filename);
						file = new File(timestamp + filename);
						fileClient.readToFile(file.getName(), true);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);*/
					} 
			}

			} catch(Exception e){
				e.printStackTrace();
			}
		 }
	}

/*
	 private void addReportFilesFromSubDir(String reportId, String dirName, DataLakeDirectoryClient directoryClient, String timestamp, List<File> files) {
		 DataLakeDirectoryClient client=directoryClient.getSubdirectoryClient(dirName).getSubdirectoryClient(reportId);
		 if(client != null){
			 logger.info("loading "+ dirName +"  for reportid  "+ reportId);
			 PagedIterable<PathItem> pagedIterable = client.listPaths();
			 java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
			String filename;
			File file;
			PathItem item;
			DataLakeFileClient fileClient;
			try{
			 while (iterator.hasNext()) {
				  item = iterator.next();
					if (item!=null && !item.isDirectory()) {
						logger.info(item.getName());
						filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
						logger.info(filename);
						fileClient = client.getFileClient(filename);
						file = new File(timestamp + filename);
						fileClient.readToFile(file.getName(), true);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);
					} 
			}

			} catch(Exception e){
				e.printStackTrace();
			}
		 }
	}*/


	private boolean matchingBlobId(Set<String> blobIds, String filename) {
		// pr_mig_1382506_ARGUS SR 1-13583102941.pdf
	/*	 
		 String blobid =filename.substring(7,filename.indexOf('_',7));
		 logger.info("**************check blob id in report blob set**** "+ blobid);
		 logger.info(blobIds.toString());
		 return blobIds.contains(blobid);*/
		
		return true;
		 
	}

	private static void copyInputStreamToFile(InputStream inputStream, File file)
	            throws IOException {

	        // append = false
		       FileOutputStream outputStream = new FileOutputStream(file, false);
	        try  {
	            int read;
	            byte[] bytes = new byte[8192];
	            while ((read = inputStream.read(bytes)) != -1) {
	                outputStream.write(bytes, 0, read);
	            }
	        }finally{
	        	outputStream.close();
	        }

	    }
	
	
	  // trusting all certificate 
	 public void doTrustToCertificates() throws Exception {
	        Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
	        TrustManager[] trustAllCerts = new TrustManager[]{
	                new X509TrustManager() {
	                    public X509Certificate[] getAcceptedIssuers() {
	                        return null;
	                    }

	                    public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
	                        return;
	                    }

	                    public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
	                        return;
	                    }
	                }
	        };

	        SSLContext sc = SSLContext.getInstance("SSL");
	        sc.init(null, trustAllCerts, new SecureRandom());
	        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	        HostnameVerifier hv = new HostnameVerifier() {
	            public boolean verify(String urlHostName, SSLSession session) {
	                if (!urlHostName.equalsIgnoreCase(session.getPeerHost())) {
	                    System.out.println("Warning: URL host '" + urlHostName + "' is different to SSLSession host '" + session.getPeerHost() + "'.");
	                }
	                return true;
	            }
	        };
	        HttpsURLConnection.setDefaultHostnameVerifier(hv);
	    }
	 
	  // connecting to URL
	    public void connectToUrl(String u){
	     try {
			doTrustToCertificates();
			 URL url = new URL(u);
		     HttpURLConnection conn = (HttpURLConnection)url.openConnection(); 
		     System.out.println("ResponseCode ="+conn.getResponseCode());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//  
	    
	   }
	    
	    
	    public List<File>  getDownloadUsingAzureBlobStorageClient(List<String>filePaths) {
	    	Date d = new Date(System.currentTimeMillis());
	    	String timestamp = d.toString().replaceAll(":", "_");
			List<File> files = new ArrayList<>();
			BlobContainerClient bclient = AzureBlobStorageClient.getAzureBlobContainerClient("protusfiles");//("hipi");
			//bclient.getBlobClient("PPIFiles")
			System.out.println("***************************bclient**************" +bclient);
			logger.info("***************************bclient**************"+bclient);
			BlobClient bc ;
            File file;
			String filename;
			for(String filePath: filePaths) {
				bc = bclient.getBlobClient(filePath);
				if (bclient != null) {
					logger.info(bc.getBlobUrl());
					filename = filePath.substring(filePath.lastIndexOf("/") + 1);
					logger.info(filename);
					file = new File( timestamp+filename);
					bc.downloadToFile(file.getPath(),true);
					files.add(file);
				}
			}
			return files;
		}


}
