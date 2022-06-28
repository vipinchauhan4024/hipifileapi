package com.volvo.hipi.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.volvo.hipi.azureservices.DownloadFromAzure;
import com.volvo.hipi.azureservices.UploadToBlobStorage;
import com.volvo.hipi.helper.AzureBlobStorageClient;

@RestController
@RequestMapping("api/v1/")
public class FileStreamController {

	@Autowired
	private UploadToBlobStorage uploadToBlobStorage;
	
	@Autowired
	private DownloadFromAzure downloadFromAzure;

	@GetMapping("hello")
	public String hello() {
		return "hello form hipi file services";
	
	}
	
	@GetMapping("Hi")
	public String hi() {
		return "hi";
	
	}

	@GetMapping("uploadAttachments")
	@RequestMapping(value = "uploadAttachments")
	public String uploadAttachments(@RequestParam int reportid,@RequestParam int reportno, @RequestParam String dir) {
		String msg = " Uploaded report attachment  ";
		try {
			String folderpath =dir+"/"+reportno+"/";

			uploadToBlobStorage.loadAttachments(reportid,reportno,folderpath, null);
		} catch (Exception e) {
			msg = e.getMessage();
		}
		return msg;
	}
	
	@GetMapping("uploadFailedAttachments")
	@RequestMapping(value = "uploadFailedAttachments")
	public String uploadFailedAttachments( @RequestParam String dir) {
		String msg = " Uploaded Failed ";
		try {
			uploadToBlobStorage.reUploadFailedAttchments(dir, true);
		} catch (Exception e) {
			msg = e.getMessage();
		}
		return msg;
	}
	
	
	//@GetMapping("uploadAttachmentsFromDb")
	//@RequestMapping(value = "uploadAttachmentsFromDb")
	public String uploadAttachmentsFromDb(@RequestParam int minReportId, @RequestParam int maxReportId,@RequestParam String dir) {
		String msg = " Uploaded report from report scope where reportid >= "+ minReportId+" and  reportid <= "+ maxReportId;
		try {
			uploadToBlobStorage.loadAttachmentsFromDb(minReportId, maxReportId, dir);
		} catch (Exception e) {
			msg = e.getMessage();
			
		}
		System.out.println("***************"+msg);
		return msg;
	}

	@GetMapping("downloadattachments/MPI")
	@RequestMapping(value = "downloadattachments/MPI", produces = "application/zip")
	public ResponseEntity<StreamingResponseBody> downlaodMPIAttachments(@RequestParam String reportno ,@RequestParam String reportid) throws IOException {
		return downloadReportZipAndSend(reportno,reportid,"MPI");
	}
	
	@GetMapping("downloadattachments/PPI")
	@RequestMapping(value = "downloadattachments/PPI", produces = "application/zip")
	public ResponseEntity<StreamingResponseBody> downlaodPPIAttachments(@RequestParam String reportno ,@RequestParam String reportid) throws IOException {
		return downloadReportZipAndSend(reportno,reportid,"PPI");
	}
	
	private ResponseEntity<StreamingResponseBody> downloadReportZipAndSend(String reportno ,String reportid, String reportType) {

		return ResponseEntity.ok().header("Content-Disposition", "attachment; filename=\"" + reportid + ".zip\"")
				.body(out -> {
					System.out.println("Download for file started" + reportid);
					List<File> files = null;
					try {
						files = downloadFromAzure.getAttachmentsFromAzureBlob( reportno,reportid, reportType);
					    ZipOutputStream zipOutputStream = new ZipOutputStream(out);
						
					// create a list to add files to be zipped
					

					// package files
					for (File file : files) {
						// new zip entry and copying inputstream with file to
						// zipOutputStream, after all closing streams
						zipOutputStream.putNextEntry(new ZipEntry(file.getName().substring(file.getName().indexOf("pr_mig"))));
						FileInputStream fileInputStream = new FileInputStream(file);
						IOUtils.copy(fileInputStream, zipOutputStream);
						fileInputStream.close();
						zipOutputStream.closeEntry();
					}
					zipOutputStream.close();
					System.out.println("downlaod finish");
					} catch (Exception e) {
						e.printStackTrace();
						String text = "Error occured sorry !! may be report does not have attachments";
						out.write(text.getBytes());
						System.out.println(e.getMessage());
					}
					finally{
						if (files != null ){
							for (File file : files) {
								if (file.exists()) {
									file.delete();
								}
							}
						}
					}
				});

	
	}

	@GetMapping("blobCount")
	@RequestMapping(value = "blobcount")
	public int getBlobCount() throws IOException {
		System.out.println("getting count : ");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();
		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("protusfiles");

		
		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient("MPIFiles");
		PagedIterable<PathItem> pageddirIterable= directoryClient.listPaths();
		java.util.Iterator<PathItem> iterator1 = pageddirIterable.iterator();
		int count =0;
		while (iterator1.hasNext()){
			System.out.println(iterator1.next().getName());
			count ++;
		}
		return count;
	}

	
	

	@GetMapping(path = "greeting")
	public String getGreeting() {
		return "Hello Azure demo app Service is running Ok!! ";
	}

}
