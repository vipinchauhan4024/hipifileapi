package com.volvo.hipi.controller;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.volvo.hipi.UploadToBlobStorage;
import com.volvo.hipi.helper.AzureBlobStorageClient;
import com.volvo.hipi.helper.Constants;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@RestController
@RequestMapping("api/v1/")
public class FileStreamController {

     @Autowired
     private UploadToBlobStorage uploadToBlobStorage;
    @GetMapping("filedownlaod")
    public String hello() {
        BlobServiceClient blobServiceClient = AzureBlobStorageClient.getAzureBlobStorageClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(Constants.CONTAINERNAME);
        BlobClient blobClient = blobContainerClient.getBlobClient("docker.png");
        String filePath = "C:\\view\\gitproject\\hipifileapi\\download\\";
        blobClient.downloadToFile(filePath + blobClient.getBlobName(), true);
        return "file downloaded";
    }

    @GetMapping("uploadAttachments")
    @RequestMapping(value = "uploadAttachments")
    public String uploadAttachments(@RequestParam int  reportid ){
        String msg=" Uploaded  ";
        try {
            uploadToBlobStorage.loadAttachments(reportid);
        } catch (Exception e ) {
            msg =e.getMessage();
        }
        return msg;
    }

    @GetMapping("downloadattachments")
    @RequestMapping(value = "downloadattachments", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> downlaodAttachments(@RequestParam String  reportid ) throws IOException {
        return ResponseEntity
                .ok()
                .header("Content-Disposition", "attachment; filename=\""+reportid+".zip\"")
                .body(out -> {
                    var zipOutputStream = new ZipOutputStream(out);
                    // create a list to add files to be zipped
                    System.out.println("Download for file started"+ reportid);
                    List<File> files = getAttachmentsFromAzureBlob(reportid);

                    // package files
                    for (File file : files) {
                        //new zip entry and copying inputstream with file to zipOutputStream, after all closing streams
                        zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
                        FileInputStream fileInputStream = new FileInputStream(file);
                        IOUtils.copy(fileInputStream, zipOutputStream);
                        fileInputStream.close();
                        zipOutputStream.closeEntry();
                    }
                    zipOutputStream.close();

                    for (File file : files) {
                        file.delete();
                    }
                    System.out.println("downlaod finish");
                });

    }


    private List<File> getAttachmentsFromAzureBlob(String reportNo) {
        Date d = new Date(System.currentTimeMillis());
        String timestamp = d.toString().replaceAll(":", "_");

        BlobServiceClient blobServiceClient = AzureBlobStorageClient.getAzureBlobStorageClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(Constants.CONTAINERNAME);
        ListBlobsOptions op = new ListBlobsOptions();
        op.setPrefix(reportNo + Constants.SUFFIX);
        PagedIterable<BlobItem> items = blobContainerClient.listBlobs(op, null);
        List<File> files = new ArrayList<>();
        BlobClient blobClient;
        for (BlobItem item : items) {

            blobClient = blobContainerClient.getBlobClient(item.getName());

            //   .forEach(blobItem -> System.out.println("Blob name: " + blobItem.getName() + ", Snapshot: " + blobItem.getSnapshot()));
            String filePath = Constants.BASE_PATH + timestamp + blobClient.getBlobName();
            System.out.println(filePath);
            blobClient.downloadToFile(filePath, true);
            File f = new File(filePath);
            files.add(f);
        }

        return files;
    }


    @GetMapping(path = "greeting")
    public String getGreeting() {
        return "Hello Azure demo app Service is running Ok!! ";
    }

}
