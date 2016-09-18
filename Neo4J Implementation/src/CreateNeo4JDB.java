import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;


public class CreateNeo4JDB {
	
	 public static final String userFile = "u.user";
	  public static final String userdataFile = "u.data";
	  public static final String genreFile = "u.genre";
	  public static final String infoFile = "u.info";
	  public static final String itemFile = "u.item";
	  public static final String occupationFile = "u.occupation";

	  // input folder containing the data files
	  public static String inputPath = null; // please end with a trailing '/' as shown
	  
	  // folder where the db will be formed
	  public static String dbPath = null;

	public static void main(String args[]) {
		
		Properties prop = new Properties();
		String propFileName = "database.properties";
		InputStream inputStream = CreateNeo4JDB.class.getResourceAsStream("database.properties");
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
		
		Scanner scanner = new Scanner(System.in);
		int choice = 0;
		System.out
				.println("Welcome.. Enter your choice \n1 to create database\n2 to execute queries\n3 to exit");
		do {

			choice = scanner.nextInt();
			if (choice == 1)
				new CreateNeo4JDB().load();
			else if (choice == 2)
				new CreateNeo4JDB().query(scanner);
			else if (choice == 3) {
				System.out.println("bye..");
				System.exit(0);
			} else
				System.out.println("Wrong choice.. Try again");
			System.out
					.println("Welcome.. Enter your choice \n1 to create database\n2 to execute queries\n3 to exit");
		} while (choice != 3);
		scanner.close();

	}

	public void load() {

		

		try {
			long startTime = System.currentTimeMillis();
			loadDB();
			long endTime = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			System.out.println("Time Taken:"+totalTime);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void query(Scanner scanner) {

		Neo4JRecommend q = new Neo4JRecommend();

		int choice;
		String mid,rating;
		System.out
				.println("Enter your query choice\n"
						+ "1 Collaborative Filtering\n"
						+ "2 Item Based Filtering\n"
						+ "3 Quit \n"
						+ "4 To go to main menu");
		
		do {

			choice = scanner.nextInt();
			switch(choice){
			
			case 1:
				System.out.println("Collaborative Filtering");
				System.out.println("Enter User id:");
				mid = scanner.next();
				System.out.println("Enter the Rating:");
				rating=scanner.next();
				long startTime = System.currentTimeMillis();
				q.query1(mid,rating);
				long endTime = System.currentTimeMillis();
				long totalTime = endTime - startTime;
				System.out.println("Time Taken:"+totalTime);
				break;
			
			case 2:
				System.out.println("ItemBased Filtering");
				System.out.println("Enter User id:");
				mid = scanner.next();
				long startTime1 = System.currentTimeMillis();
				q.query2(mid);
				long endTime1 = System.currentTimeMillis();
				long totalTime1 = endTime1 - startTime1;
				System.out.println("Time Taken:"+totalTime1);
				break;
			
			case 3:
				System.exit(0);
				break;
					
			case 4:
				return;
				
			default:
				System.out.println("Wrong choice.. Try again");
				
			}
			System.out
			.println("Enter your query choice\n"
					+ "1 Collaborative Filtering\n"
					+ "2 Item Based Filtering\n"
					+ "3 Quit \n"
					+ "4 To go to main menu");
				
		} while (choice != 5);

		scanner.close();

	}
	

	 BufferedReader breader;

	  Map<String, Long> userMap = new HashMap<String, Long>();
	  Map<String, Long> ItemMap = new HashMap<String, Long>();
	  

	  public void loadDB() throws IOException {

	    deleteDataDir(new File(dbPath));

	    loaduser();

	    loadGenres();

	    //loadInfo();

	    loadItem();

	    loadOccupation();
	    
	    loaduserDataRel();

	    System.out.println("Done loading..Shutting down....");
	  }

	  public void loaduser() throws IOException {

		  
	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);
	      
	      // indexes are only created for the id attribute of the movie.
	      // Indexes are dependent on the query.
	      // So create as many indexes as the queries demand
	      inserter.createDeferredSchemaIndex(MyLabels.USER).on("userid").create();

	      Map<String, Object> properties = new HashMap<>();

	      String line;
	      // read movies
	      breader = new BufferedReader(new FileReader(inputPath + userFile));
	      while ((line = breader.readLine()) != null) {
	    	  
	    	  
	        String[] parts = line.split("\\|");
	        // load User Data
	        properties.put("userid", parts[0]);
	        properties.put("age", (parts[1]));
	        properties.put("gender", parts[2]);
	        properties.put("occupation", parts[3]);
	        properties.put("zipcode", parts[4]);

	        long node = inserter.createNode(properties, MyLabels.USER);
	        userMap.put(parts[0], node);
	        
	/*        for (Map.Entry<String, Object> entry : properties.entrySet()) 
	        {
	      	  
	      	  System.out.println("Key"+entry.getKey()+", Value"+entry.getValue());
	        }*/

	      }
	      

	    } finally {
	      if (inserter != null) {
	        inserter.shutdown();
	      }
	    }
	  }

	  public void loadGenres() throws IOException {
	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);

	      
	      Map<String, Object> properties = new HashMap<>();
	      breader = new BufferedReader(
	                      new FileReader(inputPath + genreFile));
	      String line;
	      while ((line = breader.readLine()) != null) {
	        String[] parts = line.split("\\|");
	        //load Genres
	        
	        
	        properties.put("GenreName", parts[0]);
	        properties.put("GenreId", Integer.parseInt(parts[1]));
	        
	        long node = inserter.createNode(properties, MyLabels.GENRE);
//	        
	      }
	      
	    } finally {
	      if (inserter != null) {
	        inserter.shutdown();
	      }
	    }
	  }

	  public void loadInfo() throws IOException {

	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);

	      Map<String, Object> properties = new HashMap<>();

	      breader = new BufferedReader(
	                      new FileReader(inputPath + infoFile));
	      String line;
	      while ((line = breader.readLine()) != null) {
	        String[] parts = line.split("\\s");
	        // load Info
	        properties.put("number", parts[0]);
	        properties.put("category", parts[1]);

	        long node = inserter.createNode(properties, MyLabels.INFO); 
	  //      screenMap.put(parts[0], node);
	      }
	    } finally {
	      if (inserter != null) {
	        inserter.shutdown();
	      }
	    }
	  }

	  public void loadItem () throws IOException {
	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);
	      Map<String, Object> properties = new HashMap<>();
	      breader = new BufferedReader(
	                      new FileReader(inputPath + itemFile));
	      String line;
	      
	      
	      
	      while ((line = breader.readLine()) != null) {
	        String[] parts = line.split("\\|");
	       // System.out.println(parts[2]);
	        properties.put("movieId", parts[0]);
	        properties.put("title", parts[1]);
	        
	     //   String z= (String)(parts[2]);
	        String sub=parts[2].substring(parts[2].lastIndexOf("/")+1, parts[2].length());
	       // System.out.println(sub);
	        properties.put("ReleaseDate", parts[2]);
	        properties.put("videoReleaseDate", sub);
	        properties.put("IMDBURL", parts[4]);
	        properties.put("unknown", parts[5]);
	        properties.put("Action", parts[6]);
	        properties.put("Adventure", parts[7]);
	        properties.put("Animation", parts[8]);
	        properties.put("Childrens", parts[9]);
	        properties.put("Comedy", parts[10]);
	        properties.put("Crime", parts[11]);
	        properties.put("Documentary", parts[12]);
	        properties.put("Drama", parts[13]);
	        properties.put("Fantasy", parts[14]);
	        properties.put("FilmNoir", parts[15]);
	        properties.put("Horror", parts[16]);
	        properties.put("Musical", parts[17]);
	        properties.put("Mystery", parts[18]);
	        properties.put("Romance", parts[19]);
	        properties.put("SciFi", parts[20]);
	        properties.put("Thriller", parts[21]);
	        properties.put("War", parts[22]);
	        properties.put("Western", parts[23]);

	        long node = inserter.createNode(properties, MyLabels.ITEM); 
	        ItemMap.put(parts[0], node);

	      }
	      
	   
	    } finally {
	      if (inserter != null) {
	        inserter.shutdown();
	      }
	    }
	  }

	  public void loadOccupation() throws IOException {

	    BatchInserter inserter = null;
	    try {
	      inserter = BatchInserters.inserter(dbPath);

	      breader = new BufferedReader(
	                      new FileReader(inputPath + occupationFile));
	      String line;
	      while ((line = breader.readLine()) != null) {
	        Map<String, Object> properties = new HashMap<String, Object>();
	        String[] parts = line.split("");
	        properties.put("Occupation", parts[0]);
	        long node = inserter.createNode(properties, MyLabels.OCCUPATION); 
	  
	      }
	    } finally {
	      if (inserter != null) {
	        inserter.shutdown();
	      }
	    }
	  }
	  
	  public void loaduserDataRel() throws IOException {
		    BatchInserter inserter = null;
		    try {
		      inserter = BatchInserters.inserter(dbPath);
		      Map<String, Object> properties = new HashMap<>();
		      breader = new BufferedReader(
		                      new FileReader(inputPath + userdataFile));
		      String line;
		      while ((line = breader.readLine()) != null) {
		        String[] parts = line.split("\\s+");

		        Long user = userMap.get(parts[0]);
		        Long item = ItemMap.get(parts[1]);
		        
		        
		 
		        properties.put("Rating", parts[2]);
		        properties.put("TimeStamp", parts[3]);
		        
		        inserter.createRelationship(user, item, RelTypes.HAS_ITEM, properties);
		        
	/*	        
		        for (Entry<String, Object> entry : properties.entrySet()) 
		        {
		      	  
		      	  System.out.println(entry.getKey()+","+entry.getValue());
		        }*/
		      }
		    } finally {
		      if (inserter != null) {
		        inserter.shutdown();
		        
		      }
		    }
		  }

	  public void deleteDataDir(File dir) {
	    for (File file : dir.listFiles())
	      file.delete();
	  }

	
	  public enum MyLabels implements Label {
		  USER, 
		  GENRE, 
		  INFO,
		  ITEM,
		  OCCUPATION;
		}
	  
	  public enum RelTypes implements RelationshipType {
		  HAS_ITEM, 
		  PLAYS_MOVIE;
		}
}
