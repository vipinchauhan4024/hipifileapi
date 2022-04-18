package com.volvo.hipi;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.volvo.hipi.helper.AzureBlobStorageClient;
import com.volvo.hipi.helper.Constants;
import com.volvo.hipi.hipifileapi.BossDbConnection;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;

@Service
public class UploadToBlobStorage {



    public void testDb() throws ClassNotFoundException, SQLException, IOException {
        //Loading the required JDBC Driver class
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        //Creating a connection to the
        Connection conn = DriverManager.getConnection("jdbc:jtds:sqlserver://SEGOTN13190/DPROTUST:1433", "protusadm", "protustest");

        //Executing SQL query and fetching the result
        Statement st = conn.createStatement();
        String sqlStr = "select * from  blobdata  where blobdataid =408";
        ResultSet rs = st.executeQuery(sqlStr);
        String fname = "";
        while (rs.next()) {
            fname = rs.getString("BLOBNAME");
            System.out.println(fname);
            Blob blob = rs.getBlob("BLOBDATA");
            InputStream in = blob.getBinaryStream();
            File someFile = new File(fname);
            OutputStream out = new FileOutputStream(someFile);
            byte[] buff = new byte[4096];  // how much of the blob to read/write at a time
            int len = 0;
            while ((len = in.read(buff)) != -1) {
                out.write(buff, 0, len);
            }


        }
    }
    public void loadAttachments(int reportId) throws SQLException, IOException {
        Connection conn = BossDbConnection.getDbConnection();
        System.out.println("loading attachments for "+ reportId);
        String qry = "Select rd.ReportID, rd.BlobDataID, b.BLOBNAME, b.blobtype,b.blobsize,b.BLOBDATA from ReportDocument rd, blobdata b where" +
                " rd.BlobDataID = b.BlobDataID and rd.ReportID in (?)";

        PreparedStatement stmt = conn.prepareStatement(qry);
        stmt.setInt(1, reportId);

        ResultSet rs = stmt.executeQuery();
        String bname = "";
        BlobData bdata;
        boolean createDir = true;
        while (rs.next()) {
            bname = rs.getString("BLOBNAME");
            System.out.println(bname);
            bdata = new BlobData(bname, rs.getString("blobtype"), rs.getInt("BlobSize"),rs.getBlob("BLOBDATA"));
            uploadAttachment(bdata, reportId+"");
//            InputStream in = blob.getBinaryStream();
//            File someFile = new File(fname);
//            OutputStream out = new FileOutputStream(someFile);
//            byte[] buff = new byte[4096];  // how much of the blob to read/write at a time
//            int len = 0;
//            while ((len = in.read(buff)) != -1) {
//                out.write(buff, 0, len);
//            }
           // if (createDir) {

          //  } else {
            //    uploadAttachment(someFile, null);
            //}
        }

    }

    void uploadAttachment(BlobData blobData, String reportId) throws SQLException, IOException {
        BlobServiceClient blobServiceClient = AzureBlobStorageClient.getAzureBlobStorageClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(Constants.CONTAINERNAME);
        BlockBlobClient blobClient= blobContainerClient.getBlobClient(reportId+ Constants.SUFFIX+blobData.getBlobName()).getBlockBlobClient();
        InputStream dataStream = new ByteArrayInputStream(blobData.getBlob().getBytes(1, blobData.getBlobSize()));
        blobClient.upload(dataStream, blobData.getBlobSize());
        dataStream.close();
    }
}

class BlobData {
    private String blobName;
    private String blobType;
    private Blob blob;
    private int blobSize;

    public BlobData(String blobName, String blobType, int blobSize, Blob blob) {
        this.blob=blob;
        this.blobName =blobName;
        this.blobType =blobType;
        this.blobSize =blobSize;
    }

    public Blob getBlob() {
        return blob;
    }

    public void setBlob(Blob blob) {
        this.blob = blob;
    }

    public String getBlobName() {
        return blobName;
    }

    public void setBlobName(String blobName) {
        this.blobName = blobName;
    }

    public String getBlobType() {
        return blobType;
    }

    public void setBlobType(String blobType) {
        this.blobType = blobType;
    }

    public int getBlobSize() {
        return blobSize;
    }

    public void setBlobSize(int blobSize) {
        this.blobSize = blobSize;
    }
}