import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;



public class CreateMySQLDB {

  static BufferedReader breader;
  public static final String dataFile = "u.data";
  public static final String genreFile = "u.genre";
  public static final String infoFile = "u.info";
  public static final String itemFile = "u.item";
  public static final String occupationFile = "u.occupation";
  public static final String userFile = "u.user";

  // input folder containing the data files
  public static String inputPath = null; // please end with a trailing '/' as shown
  
  // folder where the db will be formed
  public static String dbPath = null;

  public static void main(String [] args) throws IOException {
	  
	  
		Properties prop = new Properties();
		String propFileName = "database.properties";
		InputStream inputStream = MySQLRecommend.class.getResourceAsStream("database.properties");
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		inputPath = prop.getProperty("inputPath");
		dbPath = prop.getProperty("dbPath");
		//String password = prop.getProperty("password");
	  
	  
	  
		long startTime = System.currentTimeMillis();
    deleteDataDir(new File(dbPath));
    occupation();
     user();
     genres();
    item();
     moviegenre();
    data();
    info();
    System.out.println("Done loading..Shutting down....");
    long endTime = System.currentTimeMillis();
	long totalTime = endTime - startTime;
	System.out.println("total execution time"+totalTime);
  }
  
  public static Connection connection(){
	  
	  
	  String myDriver = "com.mysql.jdbc.Driver";
      String myUrl = "jdbc:mysql://localhost:3306/movierec";
      Connection conn = null;
      try {
		Class.forName(myDriver);
		conn = DriverManager.getConnection(myUrl, "root", "");
		
	} catch (ClassNotFoundException | SQLException e) {
		
		e.printStackTrace();
	}
      
	  return conn;
	  
  }

  public static void occupation() throws IOException {
	  
	  Connection conn = connection();

    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(dbPath);

      String line;
      // read movies
      breader = new BufferedReader(
                      new FileReader(inputPath + occupationFile));
      String query = " insert into occupation (occupation) values (?)";
      try{
      while ((line = breader.readLine()) != null) {
    	  PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString (1, line);
		
			preparedStmt.execute();
      }
    }
      catch (SQLException e) {
  		
  		e.printStackTrace();
  	}
    } 
    finally {
        if (inserter != null) {
          inserter.shutdown();
          
        }
        try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
    } 
    }
  

  public static void user() throws IOException {
	  
	  Connection conn = connection();

    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(dbPath);

      String line;
      // read movies
      breader = new BufferedReader(
                      new FileReader(inputPath + userFile));
      String query = " insert into user (userid,age,gender,occupation,zipcode) values (?,?,?,?,?)";
     try{
      while ((line = breader.readLine()) != null) {
    	  String [] parts = line.split("\\|");
    	 
    	  int id = Integer.parseInt(parts[0]);
    	  int age = Integer.parseInt(parts[1]);
    	  
    	  PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setInt (1, id);
			preparedStmt.setInt (2, age);
			preparedStmt.setString (3, parts[2]);
			preparedStmt.setString (4, parts[3]);
			preparedStmt.setString (5, parts[4]);
			preparedStmt.execute();
      }
   }
      catch (SQLException e) {
  		
  		e.printStackTrace();
  	}
    } 
    finally {
        if (inserter != null) {
          inserter.shutdown();
          
        }
        try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
    } 
    }

  public static void genres() throws IOException {
	  
	  Connection conn = connection();

    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(dbPath);


      String line;
      // read movies
      breader = new BufferedReader(
                      new FileReader(inputPath + genreFile));
      String query = " insert into genre (sno,genre) values (?,?)";
      try{
      while ((line = breader.readLine()) != null) {
    	  PreparedStatement preparedStmt = conn.prepareStatement(query);
    	  String [] parts = line.split("\\|");
    	  if(parts.length == 2){
    	  //System.out.println(parts.length+parts[0]);
    	  int sno = Integer.parseInt(parts[1]);
			preparedStmt.setInt (1, sno);
			preparedStmt.setString (2, parts[0]);
		
		preparedStmt.execute();
    	  }
      }
    }
      catch (SQLException e) {
  		
  		e.printStackTrace();
  	}
    } 
    finally {
        if (inserter != null) {
          inserter.shutdown();
          
        }
        try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
    } 
    }


public static void item() throws IOException {
	  
	  Connection conn = connection();

    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(dbPath);
      String line;
      // read movies
      breader = new BufferedReader(
                      new FileReader(inputPath + itemFile));
      String query = " insert into item (movieid,movietitle,videoreleasedate,IMDBurl) values (?,?,?,?)";
      try{
      while ((line = breader.readLine()) != null) {
    	  PreparedStatement preparedStmt = conn.prepareStatement(query);
    	  String [] parts = line.split("\\|");
    	  java.util.Date date = new Date(0);
    	  if(parts[2].contains("19")){
    		  //System.out.println("hello");
    	  date = new SimpleDateFormat("dd-MMM-yyyy").parse(parts[2]);} 
    	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    	  String date1 =sdf.format(date);
    	  //System.out.println(date1);

    	 int movieid = Integer.parseInt(parts[0]);
    	  preparedStmt.setInt (1, movieid);
    	 preparedStmt.setString (2, parts[1]);
    	 preparedStmt.setString(3, date1);
    	 preparedStmt.setString (4, parts[4]);
		
    	  preparedStmt.execute();
    	  
      }
      }
      catch (SQLException e) {
  		
  		e.printStackTrace();
  	} catch (ParseException e) {

		e.printStackTrace();
	}
      }
    finally {
        if (inserter != null) {
          inserter.shutdown();
          
        }
        try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
    } 
    }
public static void moviegenre() throws IOException {
	  Connection conn = connection();

	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);
	      String line;
	      // read movies
	      breader = new BufferedReader(
	                      new FileReader(inputPath + itemFile));
    String query = " insert into moviegenre (movieid,genre) values (?,?)";
    try{
    while ((line = breader.readLine()) != null) {
    	String [] parts = line.split("\\|");
    	for(int i=5; i<=23;i++){
    		
    	if(Integer.parseInt(parts[i]) == 1) 
    		{
    		//System.out.println(parts[i]);
    		String genre = null;
    		java.sql.Statement st=conn.createStatement();
    		int sno = i-5;
    		//System.out.println(sno);
    		ResultSet rs = st.executeQuery("select genre from genre where sno ="+sno);
    		while(rs.next()){
    			
    			//System.out.println(rs.getString("genre"));
    			genre = rs.getString("genre");
    		}
    		//System.out.println(genre);
    		PreparedStatement preparedStmt = conn.prepareStatement(query);
    		preparedStmt.setString (1, parts[0]);
    		preparedStmt.setString (2, genre);
    		preparedStmt.execute();
    		}
    	
    		}
    	}
  }
    catch (SQLException e) {
		
		e.printStackTrace();
	}
  } 
  finally {
      if (inserter != null) {
        inserter.shutdown();
        
      }
      try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
  } 
  }

public static void data() throws IOException {
	  
	  Connection conn = connection();

  BatchInserter inserter = null;
  try {
    inserter = BatchInserters.inserter(dbPath);

    breader = new BufferedReader(
                    new FileReader(inputPath + dataFile));
    String query = "LOAD DATA INFILE '" + inputPath + dataFile +
                            "' INTO TABLE data" +
                            " COLUMNS TERMINATED BY '\\t'"+
                            " LINES TERMINATED BY '\\n'";
    try{

    	java.sql.Statement st=conn.createStatement();
    	st.execute(query);
    	
  }
    catch (SQLException e) {
		
		e.printStackTrace();
	}
  }
  finally {
      if (inserter != null) {
        inserter.shutdown();
        
      }
      try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
  } 
  }

public static void info() throws IOException {
	  
	  Connection conn = connection();

BatchInserter inserter = null;
try {
  inserter = BatchInserters.inserter(dbPath);

  String line;
  // read movies
  breader = new BufferedReader(
                  new FileReader(inputPath + infoFile));
  String query = " insert into info (count,content) values (?,?)";
  try{
  while ((line = breader.readLine()) != null) {
  	String [] parts = line.split(" ");
  	int count = Integer.parseInt(parts[0]);
  	
 
	  PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setInt (1, count);
			preparedStmt.setString (2, parts[1]);
			preparedStmt.execute();
  }
}
  catch (SQLException e) {
		e.printStackTrace();
	}
}
finally {
    if (inserter != null) {
      inserter.shutdown();
      
    }
    try {
			conn.close();
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
	} 
	}
  public static void deleteDataDir(File dir) {
    for (File file : dir.listFiles())
      file.delete();
  }

}
