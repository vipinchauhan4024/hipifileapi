package com.volvo.hipi;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.volvo.hipi.helper.AzureBlobStorageClient;
import com.volvo.hipi.helper.BossDbConnection;
import com.volvo.hipi.helper.Constants;

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
            
            out.close();


        }
    }


	
    public void loadAttachments(int reportId, int reportNumber) throws Exception {
        Connection conn = BossDbConnection.getDbConnection();
        System.out.println("loading attachments for "+ reportId);
        String qry = "Select rd.ReportID, rd.BlobDataID, b.Blobdataid,b.BLOBNAME, b.blobtype,b.blobsize,b.BLOBDATA from ReportDocument rd WITH (NOLOCK), blobdata b WITH (NOLOCK) where" +
                " rd.BlobDataID = b.BlobDataID and rd.ReportID in (?)";

        PreparedStatement stmt = conn.prepareStatement(qry);
        stmt.setInt(1, reportId);

        ResultSet rs = stmt.executeQuery();
        String bname = "";
        BlobData bdata = null;
        String path="";
        try{
        while (rs.next()) {
            bname = rs.getString("BLOBNAME");
            int blobdataid= rs.getInt("BlobDataID");
            System.out.println(bname);
            bdata = new BlobData(bname, rs.getString("blobtype"), rs.getInt("BlobSize"),rs.getInt("Blobdataid"),rs.getBlob("BLOBDATA"));
            if(reportNumber>0){
            	uploadAttachment(bdata, reportNumber+"");
            	path= Constants.BASE_DIR+reportNumber+"/"+ Constants.PRIFIX+bdata.getBlobDataId()+"_"+bdata.getBlobName();
            	updateUploadStatus( reportId,  reportNumber,  path, blobdataid, "Uploaded_to_hipi");
            } else {
            	throw new  Exception ("Report num madatory");
            	/*uploadAttachment(bdata, reportId+"");
            	path= Constants.BASE_DIR+reportId+"/"+ Constants.PRIFIX+bdata.getBlobName();
            	updateUploadStatus(reportId,  reportNumber,  path);*/
            }
			}
		} catch (Exception e) {
			updateUploadStatus(reportId, reportNumber, path, bdata != null ? bdata.getBlobDataId(): null, "Error occured in upload");
			e.printStackTrace();
		}

    }

    void uploadAttachment(BlobData blobData, String reportId) throws SQLException, IOException {
        BlobServiceClient blobServiceClient = AzureBlobStorageClient.getAzureBlobStorageClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(Constants.CONTAINERNAME);
        BlockBlobClient blobClient = null;
        String path = Constants.BASE_DIR+reportId+"/"+ Constants.PRIFIX + blobData.getBlobDataId()+ "_" +blobData.getBlobName();
        System.out.println("uploding :" + path);
        blobClient = blobContainerClient.getBlobClient(path).getBlockBlobClient();
        
        
        InputStream dataStream = new ByteArrayInputStream(blobData.getBlob().getBytes(1, blobData.getBlobSize()));
        blobClient.upload(dataStream, blobData.getBlobSize());
        dataStream.close();
    }
	public void loadAttachmentsFromDb(int minReportid,int maxReportId) throws Exception {
		    Connection conn = BossDbConnection.getDbConnection();
	        System.out.println("load reportids ");
	        String qry = "select distinct rs.ReportID , rs.reportNo,rs.piltype from ReportDocument rd "
	        		+ " WITH (NOLOCK) left OUTER join  reportsInScope rs WITH (NOLOCK) on rd.ReportID = rs.ReportID where  rs.ReportID >= ? and rs.ReportID <= ?   order by rs.ReportID asc";
	        PreparedStatement stmt = conn.prepareStatement(qry);
	        stmt.setInt(1, minReportid);
	        stmt.setInt(2, maxReportId);
	        ResultSet rs = stmt.executeQuery();
	        List<ReportInScope> rsList = new ArrayList<>();
	        ReportInScope report;
	        Set<Integer>  loadedReports =getAlreadyUplodedReportIds();
	        while (rs.next()) {
	        	report = new ReportInScope( rs.getInt("reportNo"), rs.getString("piltype"),  rs.getInt("ReportID"));
	        	
	        	if(loadedReports.contains(report.getReportId())) {
	        		System.out.println("already loaded so skiping");
				} else {
					loadAttachments(report.getReportId(),report.getReportNo());
		        	rsList.add(report);
				}
	        	
	         
	        }
	}
	
	 private static Set<Integer> getAlreadyUplodedReportIds() throws SQLException {
		 Connection conn = BossDbConnection.getDbConnection();
	        System.out.println("load uploaded reports ");
	        String qry = "select distinct ra.ReportID  from MIG_HIPI_ReportAttachment ra WITH (NOLOCK)  "
	        		+ "where ra.ReportID >= ? order by ra.ReportID asc";
	        PreparedStatement stmt = conn.prepareStatement(qry);
	        stmt.setInt(1, 0);
	        ResultSet rs = stmt.executeQuery();
	        Set<Integer> loadedReports = new HashSet<>();
	        while (rs.next()) {
	        	loadedReports.add(rs.getInt("ReportID"));
	        	
	        }
	        return loadedReports;
	      
		
	}
	private void updateUploadStatus(int reportId, int reportno, String path, int blobDataId, String status) throws SQLException {
		Connection conn = BossDbConnection.getDbConnection();
		String qry = " INSERT INTO MIG_HIPI_ReportAttachment (Attachmentslink, ReportID, ReportNo, BlobDataId,uploadstatus) VALUES (?, ?, ?,?,?) ";
		PreparedStatement stmt = conn.prepareStatement(qry);
		stmt.setString(1, path);
        stmt.setInt(2, reportId);
        stmt.setInt(3, reportno);
        stmt.setInt(4, blobDataId);
        stmt.setString(5, status);
        stmt.executeUpdate();
	}
}

class ReportInScope implements Comparable<ReportInScope>{
	    private int reportNo;
	    private String piltype;
	    private int reportId;
	    
		public ReportInScope(int reportNo, String piltype, int reportId) {
			super();
			this.reportNo = reportNo;
			this.piltype = piltype;
			this.reportId = reportId;
		}
		public int getReportNo() {
			return reportNo;
		}
		public void setReportNo(int reportNo) {
			this.reportNo = reportNo;
		}
		public String getPiltype() {
			return piltype;
		}
		public void setPiltype(String piltype) {
			this.piltype = piltype;
		}
		public int getReportId() {
			return reportId;
		}
		public void setReportId(int reportId) {
			this.reportId = reportId;
		}
		
		
	
		@Override
		public String toString() {
			return "ReportInScope [reportNo=" + reportNo + ", reportId=" + reportId + "]";
		}
		@Override
		public int compareTo(ReportInScope o) {
			// TODO Auto-generated method stub
			return reportId - o.getReportId();
		}
	    
	    
}
class BlobData {
    private String blobName;
    private String blobType;
    private Blob blob;
    private int blobSize;
    private int blobDataId;

    public BlobData(String blobName, String blobType, int blobSize, int blobDataId, Blob blob) {
        this.blob=blob;
        this.blobName =blobName.replaceAll("%", "percentage");;
        this.blobType =blobType;
        this.blobSize =blobSize;
        this.blobDataId=blobDataId;
    }

    public Blob getBlob() {
        return blob;
    }

   

	public int getBlobDataId() {
		return blobDataId;
	}

	public void setBlobDataId(int blobDataId) {
		this.blobDataId = blobDataId;
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