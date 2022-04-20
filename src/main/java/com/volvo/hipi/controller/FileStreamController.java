package com.volvo.hipi.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.volvo.hipi.UploadToBlobStorage;
import com.volvo.hipi.helper.AzureBlobStorageClient;
import com.volvo.hipi.helper.Constants;
import java.io.FileWriter;

@RestController
@RequestMapping("api/v1/")
public class FileStreamController {

	@Autowired
	private UploadToBlobStorage uploadToBlobStorage;

	@GetMapping("filedownlaod")
	public String hello() {
		try {
			BlobServiceClient blobServiceClient = AzureBlobStorageClient.getAzureBlobStorageClient();
			BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(Constants.CONTAINERNAME);
			BlobClient blobClient = blobContainerClient.getBlobClient("docker.png");
			String filePath = "C:\\view\\gitproject\\hipifileapi\\download\\";
			blobClient.downloadToFile(filePath + blobClient.getBlobName(), true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "file downloaded";
	}

	@GetMapping("uploadAttachments")
	@RequestMapping(value = "uploadAttachments")
	public String uploadAttachments(@RequestParam int reportid) {
		String msg = " Uploaded  ";
		try {
			uploadToBlobStorage.loadAttachments(reportid,-1);
		} catch (Exception e) {
			msg = e.getMessage();
		}
		return msg;
	}
	
	@GetMapping("uploadAttachmentsFromDb")
	@RequestMapping(value = "uploadAttachmentsFromDb")
	public String uploadAttachmentsFromDb(@RequestParam int minReportId, @RequestParam int maxReportId) {
		String msg = " Uploaded report from report scope where reportid >= "+ minReportId+" and  reportid <= "+ maxReportId;
		try {
			uploadToBlobStorage.loadAttachmentsFromDb(minReportId, maxReportId);
		} catch (Exception e) {
			msg = e.getMessage();
		}
		System.out.println(msg);
		return msg;
	}

	@GetMapping("downloadattachments")
	@RequestMapping(value = "downloadattachments", produces = "application/zip")
	public ResponseEntity<StreamingResponseBody> downlaodAttachments(@RequestParam String reportid) throws IOException {
		return ResponseEntity.ok().header("Content-Disposition", "attachment; filename=\"" + reportid + ".zip\"")
				.body(out -> {
					System.out.println("Download for file started" + reportid);
					List<File> files = getAttachmentsFromAzureBlob(reportid);
					ZipOutputStream zipOutputStream = new ZipOutputStream(out);
					try{
						
					// create a list to add files to be zipped
					

					// package files
					for (File file : files) {
						// new zip entry and copying inputstream with file to
						// zipOutputStream, after all closing streams
						zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
						FileInputStream fileInputStream = new FileInputStream(file);
						IOUtils.copy(fileInputStream, zipOutputStream);
						fileInputStream.close();
						zipOutputStream.closeEntry();
					}
					zipOutputStream.close();
					System.out.println("downlaod finish");
					} catch (Exception e) {
						String text = "Error occured sorry !! may be report does not have attachments";
						out.write(text.getBytes());
					}
					finally{
						if (files != null ){
							for (File file : files) {
								file.delete();
							}
						}
					}
				});

	}

	private List<File> getAttachmentsFromAzureBlob(String reportNo) throws IOException {
		System.out.println("Downloading files for report : "+reportNo);
		Date d = new Date(System.currentTimeMillis());
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();
		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("protusfiles");

		List<File> files = new ArrayList<>();
		
		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient("files").getSubdirectoryClient(reportNo);
		PagedIterable<PathItem> pagedIterable = directoryClient.listPaths();
		java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
		PathItem item = iterator.next();
		
		DataLakeFileClient fileClient;
		File file;
		String filename;
		try{
		while (item != null) {
			System.out.println(item.getName());
			filename = item.getName().substring(item.getName().lastIndexOf("/")+1);
			System.out.println(filename);
			fileClient = directoryClient.getFileClient(filename);
			
			file = new File(timestamp + filename);
			copyInputStreamToFile(fileClient.openInputStream().getInputStream(),file);

			files.add(file);
			if (!iterator.hasNext()) {
				break;
			}
			item = iterator.next();
		}

		} catch(Exception e){
			e.printStackTrace();
		}
		return files;
	}
	
	 private static void copyInputStreamToFile(InputStream inputStream, File file)
	            throws IOException {

	        // append = false
	        try (FileOutputStream outputStream = new FileOutputStream(file, false)) {
	            int read;
	            byte[] bytes = new byte[8192];
	            while ((read = inputStream.read(bytes)) != -1) {
	                outputStream.write(bytes, 0, read);
	            }
	        }

	    }

	@GetMapping(path = "greeting")
	public String getGreeting() {
		return "Hello Azure demo app Service is running Ok!! ";
	}

}
