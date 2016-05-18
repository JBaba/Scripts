import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.ibm.db2.jcc.am.co;
import com.ibm.db2.jcc.am.ke;


public class ConnectionTest {

		String jdbcClassName = "com.ibm.db2.jcc.DB2Driver";
		// dev url -- jdbc:db2://54.172.100.27:51000/cthix
		String url = "jdbc:db2://10.118.21.180:50000/LOCALIMP:currentSchema=IE_APP_ONLINE;";
		// dev user name : nviraddc Pass : Circle.314
		String user = "ctieapp";
		String password = "Password.1";
		Connection connection = null;
		public String sql = "SELECT * FROM syscat.columns WHERE tabname LIKE 'UA_MOOD_RSPNSE' AND tabschema LIKE 'BIP' ORDER BY colno;";
		public Statement stmt = null;
		
		public ConnectionTest() {
			// TODO Auto-generated constructor stub
		}

		public ConnectionTest(String tbName) throws SQLException {

			try {
				// Load class into memory
				Class.forName(jdbcClassName);
				// Establish connection
				connection = DriverManager.getConnection(url, user, password);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (connection != null) {
					//System.out.println("Connected successfully.");
					try {
						connection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}

		public void openConnection() throws SQLException {
			try {
				// Load class into memory
				Class.forName(jdbcClassName);
				// Establish connection
				connection = DriverManager.getConnection(url, user, password);

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {

			}

		}
		
		public void insert(){
			long l = 1035825;
			int size = 3;
			
			
			
			for (long i = l; i < (l + size); i++) {
				
			}
		}
		
		public void read() throws Exception{
			
			int count = 16;
			
			long startTime = System.currentTimeMillis();
			CountDownLatch countDownLatch = new CountDownLatch(count);
			
			for (int i = 0; i < count; i++) {
				MyRsWorker mrw = new MyRsWorker(connection, i+1, countDownLatch);
				Thread t=new Thread(mrw);
				t.start();
			}
			
			countDownLatch.await();
			
			System.out.println("Done..");
			final long endTime = System.currentTimeMillis();
			System.out.println(" Total execution time: " + ((endTime - startTime)/1000) );
			
		}
		
		public void insert1() throws Exception{
			
			int count = 16;
			int filelen = 32;
			
			long startTime = System.currentTimeMillis();
			CountDownLatch countDownLatch = new CountDownLatch(filelen);
			
			
				for (int i = 17; i < filelen; i++) {
					InsertWorker mrw = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
					Thread t=new Thread(mrw);
					t.start();
					
					/*i++;
					
					Thread t1=null,t2=null,t3=null,t4=null,t5=null;
					if(i < filelen){
						InsertWorker mrw1 = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
						t1=new Thread(mrw1);
						t1.start();
					}*/
					
					/*i++;
					
					if(i < filelen){
						InsertWorker mrw2 = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
						t2=new Thread(mrw2);
						t2.start();	
					}
					
					i++;
					
					if(i < filelen){
						InsertWorker mrw3 = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
						t3=new Thread(mrw3);
						t3.start();	
					}
					
					i++;
					
					if(i < filelen){
						InsertWorker mrw4 = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
						t4=new Thread(mrw4);
						t4.start();	
					}
					
					i++;
					
					if(i < filelen){
						InsertWorker mrw5 = new InsertWorker(connection, "C:\\Users\\nviradia\\Desktop\\file\\prop\\files\\3\\"+i+".txt",0, countDownLatch,0);
						t5=new Thread(mrw5);
						t5.start();	
					}*/
					
					t.join();
					//t1.join();
					/*if(t1 != null)
					t1.join();
					t.join();
					if(t2 != null)
					t2.join();*/
				}
			
			countDownLatch.await();
			
			System.out.println("Done..");
			final long endTime = System.currentTimeMillis();
			System.out.println(" Total execution time: " + ((endTime - startTime)/1000) );
			
		}
		
		public void insertUsing() throws Exception{
			//String sql = "insert into ED_ELIG_TEMP_PART_10PART select * from ED_ELIG_TEMP";
			String sql2 = "insert into ED_ELIG_TEMP_PART select * from ED_ELIG_TEMP";
			
			openConnection();
			Statement st = connection.createStatement();
			
			//st.execute(sql);
			st.execute(sql2);
			
			System.out.println("Done.");
		}
		
		
		public HashMap<Long, String> createDate(long date,long len){
			HashMap<Long, String> map = new HashMap<Long, String>();
			
			for(long i=1; i<len+1; i++ ){
				Date d = new Date(date);
				date += 1000;
				
				SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
				//System.out.println(i+" "+dt.format(date));
				map.put(i, dt.format(date));
			}
			
			return map;
		}
		
		public void create10MilionRecords(String path,String name,String date) throws Exception {
			
		  SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
		  Date d1 = dt.parse(date);
		  
		  HashMap<Long, String> map = createDate(d1.getTime(), 38462);
			
		  File file = new File(path+"\\"+name);
	      // creates the file
	      file.createNewFile();
	      // creates a FileWriter Object
	      FileWriter writer = new FileWriter(file); 
	      
	      String tableName = "ED_ELIG_TEMP";
	      //String sql = "Insert into "+tableName+" (CASE_NUM,CREATE_DT) values ";
	      // Writes the content to the file
	      Set<Long> keySet = map.keySet();
	      for (long l : keySet) {
	    	  String sql = "Insert into "+tableName+" (CASE_NUM,CREATE_DT) values ";
		      // Writes the content to the file
	    	  sql += "("+l+",'"+map.get(l)+"');";
	    	  //System.out.println(l+" "+sql);
	    	  writer.write(sql+"\n"); 
		  }
	      
	      writer.flush();
	      writer.close();

		}
		
		public void preWriteFile() throws Exception{
			String path = "C:\\Users\\nviradia\\Desktop\\file\\prop\\files";
			
			for(int i = 0;i<26;i++){
				
				int begin = 1;
				int day = begin+i;
				int lenth = 26;
				
				String dayS = "";
				
				if((day+"").length() == 1)
					dayS = "0"+day;
				else
					dayS = day+"";
				
				FileWorker mrw = new FileWorker(dayS,path,i);
				Thread t=new Thread(mrw);
				t.start();
				
				i++;
				
				Thread t1 = null,t2 = null,t3 = null;
				
				if(i<lenth){
					day = begin+i;
					dayS = "";
					
					if((day+"").length() == 1)
						dayS = "0"+day;
					else
						dayS = day+"";
					
					FileWorker mrw1 = new FileWorker(dayS,path,i);
					t1=new Thread(mrw1);
					t1.start();
				}
				
				i++;
				
				if(i<lenth){
					day = begin+i;
					dayS = "";
					
					if((day+"").length() == 1)
						dayS = "0"+day;
					else
						dayS = day+"";
					
					FileWorker mrw2 = new FileWorker(dayS,path,i);
					t2=new Thread(mrw2);
					t2.start();
				}
				
				i++;
				
				if(i<lenth){
					day = begin+i;
					dayS = "";
					
					if((day+"").length() == 1)
						dayS = "0"+day;
					else
						dayS = day+"";
					
					FileWorker mrw3 = new FileWorker(dayS,path,i);
					t3=new Thread(mrw3);
					t3.start();
				}
				
				t.join();
				if(t1 != null)
				t1.join();
				if(t2 != null)
				t2.join();
				if(t3 != null)
				t3.join();
					
			}	
		}

		public static void main(String[] args) throws Exception {
			ConnectionTest ct = new ConnectionTest("");
			ct.openConnection();
			//ct.read();
			
			ct.insert1();
			
			//ct.preWriteFile();
			
			//ct.insertUsing();
			
		}
}

class InsertWorker implements Runnable{

	Connection connection = null;
	String fileName = "";
	ResultSet rs = null;
	CountDownLatch countDownLatch = null;
	int sleepTime;
	java.util.Queue queue1 = new java.util.LinkedList();
	java.util.Queue queue2 = new java.util.LinkedList();
	
	public InsertWorker(Connection connection,String fileName,int batchNumber,CountDownLatch countDownLatch,int sleepTime) {
		this.connection=connection;
		this.fileName = fileName;
		this.countDownLatch=countDownLatch;
		this.sleepTime = sleepTime;
		
		queue1.add("FS");
		queue1.add("M03");
		queue1.add("MA");
		queue1.add("MC");
		queue1.add("SWS");
		queue1.add("TF");
		
		queue2.add("1");
		queue2.add("2");
		queue2.add("3");
		queue2.add("4");
		
	}
	
	public String giveMe(String sCurrentLine){
        Object e1 = queue1.element();
        Object e2 = queue2.element();
        queue1.add(e1);
        queue2.add(e2);
        
        sCurrentLine = sCurrentLine.replace("CREATE_DT)", "CREATE_DT,PROGRAM_CD,GROUP_NUM)");
        sCurrentLine = sCurrentLine.replace(");", ",'"+e1+"','"+e2+"');");
		
		sCurrentLine = sCurrentLine.substring(0, sCurrentLine.length()-1);
		
		sCurrentLine = sCurrentLine.replace("ED_ELIG_TEMP", "ED_ELIG_TEMP_MDC_NEW");
		//sCurrentLine = sCurrentLine.replace("ED_ELIG_TEMP", "ED_ELIG_TEMP_NO_MDC");
		//sCurrentLine = sCurrentLine.replace("ED_ELIG_TEMP", "ED_ELIG_TEMP_MDC");
		//sCurrentLine = sCurrentLine.replace("ED_ELIG_TEMP", "ED_ELIG_TEMP_PART");
		//sCurrentLine = sCurrentLine.replace("ED_ELIG_TEMP", "ED_ELIG_TEMP_PART_10PART");
		
		queue1.remove();
		queue2.remove();
		return sCurrentLine;
	}
	
	@Override
	public void run() {
		
		BufferedReader br = null;

		try {

			/*if(sleepTime > 2 && sleepTime < 6)
				Thread.sleep((sleepTime-2)*200);
			else if(sleepTime > 2)
				Thread.sleep(sleepTime*(100*sleepTime));*/
			
			
			String sCurrentLine;
			Statement st;
			ConnectionTest ctt1 = new ConnectionTest("");
			ctt1.openConnection();
			st = ctt1.connection.createStatement();
			
			br = new BufferedReader(new FileReader(fileName));
			int ii = 0;

			while ((sCurrentLine = br.readLine()) != null) {
				
				if(ii == 0){
					ConnectionTest ctt = new ConnectionTest("");
					ctt.openConnection();
					Connection con = ctt.connection;
					st = con.createStatement();
				}

				sCurrentLine = giveMe(sCurrentLine);
				//System.out.println(ii+" "+sCurrentLine); 
				
				ii++;
				
				if(ii > 1000){
					st.addBatch(sCurrentLine);
					st.executeBatch();
					st.close();
					ii=0;
					System.out.println("1000 done.");
				}else{
					st.addBatch(sCurrentLine);
				}
			}
			 
			st.executeBatch();
			br.close();
			File f1= new File(fileName);
			f1.delete();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		countDownLatch.countDown();
		System.out.println(fileName);
		
	}
	
}

class FileWorker implements Runnable{
	String day = "";
	String path = "";
	int i=0;
	
	public FileWorker(String day,String path,int i) {
		this.day = day;
		this.path = path;
		this.i = i;
	}
	
	@Override
	public void run() {
		
		ConnectionTest ct;
		try {
			ct = new ConnectionTest();
			ct.create10MilionRecords(path, (i+1)+".txt","2016-03-"+day+" 00:00:01.0");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}

class MyRsWorker implements Runnable{
	
	Connection connection = null;
	private int batchNumber = 0;
	String sql = "select * from (select rownumber() over() rowid,t.* from IMPACT_WESB.IMPACT_ESB_AUDIT_LOG_FTI t WHERE TRANS_TMSTP>'2016-03-31' order by TRANS_TMSTP) as tab_1 where tab_1.ROWID > "+ (100*(batchNumber-1))+"and tab_1.ROWID <= "+ (100*batchNumber);
	ResultSet rs = null;
	CountDownLatch countDownLatch = null;
	
	public MyRsWorker(Connection connection,int batchNumber,CountDownLatch countDownLatch) throws SQLException {
		this.connection=connection;
		this.batchNumber=batchNumber;
		this.countDownLatch=countDownLatch;
		
		sql = "select * from (select rownumber() over() rowid,t.* from IMPACT_WESB.IMPACT_ESB_AUDIT_LOG_FTI t WHERE TRANS_TMSTP>'2016-03-31' order by TRANS_TMSTP) as tab_1 where tab_1.ROWID > "+ (1000*(batchNumber-1))+" and tab_1.ROWID <= "+ (1000*batchNumber);
		
		Statement st = connection.createStatement();
		rs = st.executeQuery(sql);
	}
	
	@Override
	public void run() {
		
		try {
			while (rs.next()) {
				StringBuffer bf = new StringBuffer();
				bf.append(rs.getString("ROWID"));
				bf.append(":");
				bf.append(rs.getString("TRANS_TMSTP"));
				bf.append(rs.getString("MSG_ID"));
				//read BLOB data
				java.sql.Blob blob = rs.getBlob(6);
				int blobLength = (int) blob.length();  
				byte[] blobAsBytes = blob.getBytes(1, blobLength);
				//convert BLOB to CLOB
				String blobToClob = new String(blobAsBytes);
				bf.append(blobToClob);

				System.out.println(bf.toString());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		countDownLatch.countDown();
		System.out.println(sql);
	}
	
}
