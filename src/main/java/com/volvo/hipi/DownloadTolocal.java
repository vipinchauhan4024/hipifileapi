package com.volvo.hipi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.volvo.hipi.helper.BossDbConnection;
import com.volvo.hipi.helper.Constants;

public class DownloadTolocal {

	   static final String path="files/";
	   
	   public static void main(String arg []) throws Exception{
		   downloadTolocal(122691,538248); 
		   System.out.println("finish ************************* ");
		   
	   }


	public static void downloadTolocal(int minReportid, int maxReportId) throws Exception {

	    Connection conn = BossDbConnection.getDbConnection();
        System.out.println("load reportids ");
        String qry = "select distinct rs.ReportID , rs.reportNo,rs.piltype from ReportDocument rd "
        		+ " WITH (NOLOCK) left OUTER join  reportsInScope rs WITH (NOLOCK) on rd.ReportID = rs.ReportID where  rs.ReportID >= ? and rs.ReportID <= ?   order by rs.ReportID asc";
        PreparedStatement stmt = conn.prepareStatement(qry);
        stmt.setInt(1, minReportid);
        stmt.setInt(2, maxReportId);
        ResultSet rs = stmt.executeQuery();
        List<ReportInScope> rsList = new ArrayList<>();
        Set<Integer> loadedReports = getAlreadyUplodedReportIds();
        ReportInScope report;
        while (rs.next()) {
        	report = new ReportInScope( rs.getInt("reportNo"), rs.getString("piltype"),  rs.getInt("ReportID"));
        	if(loadedReports.contains(report.getReportId())) {
        		System.out.println("already loaded so skiping");
			} else {
				downloadTolocalAttachments(report.getReportId(), report.getReportNo());
				rsList.add(report);
			}
           // bdata = new BlobData(bname, rs.getString("blobtype"), rs.getInt("BlobSize"),rs.getBlob("BLOBDATA"));
            //uploadAttachment(bdata, reportId+"");
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


	public static void downloadTolocalAttachments(int reportId, int reportNumber) throws Exception {
		    
	        Connection conn = BossDbConnection.getDbConnection();
	        System.out.println("loading attachments for "+ reportId);
	        String qry = "Select rd.ReportID, rd.BlobDataID, b.Blobdataid,b.BLOBNAME, b.blobtype,b.blobsize,b.BLOBDATA from ReportDocument rd WITH (NOLOCK), blobdata b WITH (NOLOCK) where" +
	                " rd.BlobDataID = b.BlobDataID and rd.ReportID in (?)";

	        PreparedStatement stmt = conn.prepareStatement(qry);
	        stmt.setInt(1, reportId);

	        ResultSet rs = stmt.executeQuery();
	        String bname = "";
	        BlobData bdata;
	        int blobdataid = 0;
	        String filePath = path; 
	        try{
	        boolean createDir = true;
	        
	        while (rs.next()) {
	            bname = rs.getString("BLOBNAME");
	            blobdataid =rs.getInt("BlobDataID");
	            System.out.println(bname);
	            bname = bname.replace("?", "");
	            bdata = new BlobData(bname, rs.getString("blobtype"), rs.getInt("BlobSize"),rs.getInt("Blobdataid"),rs.getBlob("BLOBDATA"));
	            if(reportNumber>0){
				if (createDir) {
					File theDir = new File(path + reportNumber);
					if (!theDir.exists()) {
						theDir.mkdirs();
						createDir = false;
					}
				}
				filePath =filePath+reportNumber+"/"+Constants.PRIFIX + blobdataid+ "_" +bdata.getBlobName();
	            downloadAttachment(path,bdata, reportNumber+"", reportId+"");
	            //	path= Constants.BASE_DIR+reportNumber+"/"+ Constants.PRIFIX+bdata.getBlobDataId()+"_"+bdata.getBlobName();
	              updateUploadStatus( reportId,  reportNumber,  filePath, blobdataid,"Downloaded_to_local");
	            } else {
	            	throw new  Exception ("Report num madatory");
	            	/*uploadAttachment(bdata, reportId+"");
	            	path= Constants.BASE_DIR+reportId+"/"+ Constants.PRIFIX+bdata.getBlobName();
	            	updateUploadStatus(reportId,  reportNumber,  path);*/
	            }
	        }
		    }catch(Exception e){
		    	 e.printStackTrace();
		    	 updateUploadStatus( reportId,  reportNumber,  filePath, blobdataid,"Error in local download");
		    	 
		    }

	    }
    
   

    static void downloadAttachment(String path, BlobData blobData, String reportno, String reportId) throws SQLException, IOException {
  
        path = path+reportno+"/"+Constants.PRIFIX + blobData.getBlobDataId()+ "_" +blobData.getBlobName();
        System.out.println("downloading :" + path  +"report id "+ reportId );
        File f = new File(path);
        final Path destination = Paths.get(path);
        try (
            final InputStream in = blobData.getBlob().getBinaryStream();
        ) {
            Files.copy(in, destination);
        } catch (Exception e){
        	throw e;
        }
        
       
    }
	
	private static void updateUploadStatus(int reportId, int reportno, String path, int blobDataId, String status) throws SQLException {
		Connection conn = BossDbConnection.getDbConnection();
		String qry = " INSERT INTO MIG_HIPI_ReportAttachment (Attachmentslink, ReportID, ReportNo, BlobDataId, uploadstatus) VALUES (?, ?, ?,?,?) ";
		PreparedStatement stmt = conn.prepareStatement(qry);
		stmt.setString(1, path);
        stmt.setInt(2, reportId);
        stmt.setInt(3, reportno);
        stmt.setInt(4, blobDataId);
        stmt.setString(5, status);
        stmt.executeUpdate();
	}


}
