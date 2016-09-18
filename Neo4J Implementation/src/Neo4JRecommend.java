

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4JRecommend  {
	  // input folder containing the data files
	  public static String inputPath = null; // please end with a trailing '/' as shown
	  
	  // folder where the db will be formed
	  public static String dbPath = null;
	public void query1(String mid, String rating) {
		
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

		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabase(dbPath);
		Map<Integer, String> linke = new LinkedHashMap<Integer, String>();
		Map<Integer, String> linke1 = new LinkedHashMap<Integer, String>();
		Map<Integer, String> FinalUsers = new LinkedHashMap<Integer, String>(linke1);
		Map<Integer, String> ageF = new LinkedHashMap<Integer, String>(linke);
		Map<Integer, String> USERS = new LinkedHashMap<Integer, String>();
		
		String userlist="",movieId="",age = "",rows1="", gender="",occupation="",zipcode="";
		String gender_val="",age_val="",occupation_val="",zipcode_val="", Final_val="",query="",K="";
			
		
		//-----------------------------------USER MOVIES-----------------------------------------------
			
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
								+ "user.userid ='"+mid+"' AND r.Rating='"+rating+"' return item.movieId"))

		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Map.Entry<String, Object> column : row.entrySet()) {
					movieId += "'"+column.getValue() + "',";
				}
//				userlist += "\n";
			}
			
		}
		movieId=movieId.replaceAll(",$", "");
	//	System.out.println("MOVIEID's:"+movieId);
		
		//-----------------------------------USER MOVIES-----------------------------------------------
		
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
								+ "item.movieId IN ["+movieId+"] AND r.Rating='"+rating+"' return DISTINCT user.userid"))

		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Map.Entry<String, Object> column : row.entrySet()) {
					userlist += "'"+column.getValue() + "',";
				}
//				userlist += "\n";
			}
			
		}
		userlist=userlist.replaceAll(",$", "");
		//System.out.println("USERLIST:"+userlist);
		
		
		
		//------------------------------------USER AGE-----------------------------------
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)WHERE user.userid IN['"+mid+"'] return user.age"))
					
		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				
				for (Map.Entry<String, Object> column : row.entrySet()) {
					age +=  column.getValue() + "";
				}
				age += "\n";
System.out.println("Age of the User:"+age);

				
			}
		}
		
		age=age.replaceAll("\\s+","");
		Integer temp=Integer.parseInt(age);
		Integer t1 = temp-6;
		Integer t2 = temp+6; 
		String t11=t1.toString();
		String t22=t2.toString();
/*		System.out.println(t1);
		System.out.println(t2);*/
		

		//------------------------------------USER GENDER----------------------------------------------
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)WHERE user.userid='"+mid+"' return user.gender"))
					
		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				
				for (Map.Entry<String, Object> column : row.entrySet()) {
					gender +=  column.getValue() + "";
				}
				gender += "\n";
				System.out.println("Gender of the User:"+gender);

				
			}
		}
		gender=gender.replaceAll("\\s+","");
		
		//------------------------------------USER OCCUPATION-----------------------------------
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)WHERE user.userid='"+mid+"' return user.occupation"))
					
		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				
				for (Map.Entry<String, Object> column : row.entrySet()) {
					occupation +=  column.getValue() + "";
				}
				occupation += "\n";
				System.out.println("Occupation of the User:"+occupation);
			}
		}
		occupation=occupation.replaceAll("\\s+","");
		
		

		
		//------------------------------------USER ZIPCODE-----------------------------------
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)WHERE user.userid='"+mid+"' return user.zipcode"))
					
		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				
				for (Map.Entry<String, Object> column : row.entrySet()) {
					zipcode +=  column.getValue() + "";
				}
				zipcode += "\n";
				System.out.println("Zipcode of the User:"+zipcode);
			}
		}
		zipcode=zipcode.replaceAll("\\s+","");

		
		//-----------------------------------FILTERING USER BY AGE---------------------------------------------
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER) WHERE user.age>'"+t11+"' "
								+ "AND user.age<'"+t22+"'  AND user.userid IN["+userlist+"] return user.userid, user.gender"))
						
		{	
			while (result.hasNext()) {
				Map<String, Object> row1 = result.next();
								
				for (Map.Entry<String, Object> column : row1.entrySet()) {
					rows1 +=  column.getValue() + ";";
					
				
				}
				 				rows1 += "\n";
				 				String rows11= rows1.substring(0,rows1.indexOf(";"));
				 				String rows12= rows1.substring(rows1.indexOf(";")+1,rows1.lastIndexOf(";"));
				 				linke.put(Integer.parseInt(rows12), rows11);
				 				rows1="";


			}
			
		}
	//	System.out.println("COUNT OF AGE"+linke.size());
		
		for (Entry<Integer, String> entry : linke.entrySet()) 
		{
			age_val+="'"+entry.getKey()+"',";
			
		}
		
		age_val = age_val.replaceAll(",$", "");
		
		//-----------------------------------FILTERING BY GENDER----------------------------------
		if((linke.size())> 10){
			//ageF=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) linke).clone();
			//System.out.println("AGAAA"+ageF);
			linke.clear();
	//	System.out.println("Inside Filtering gender:"+linke.size());
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER) WHERE user.userid IN["+age_val+"] "
								+ "AND user.gender='"+gender+"' return user.userid, user.occupation"))
						
		{	
			while (result.hasNext()) {
				Map<String, Object> row1 = result.next();
								
				for (Map.Entry<String, Object> column : row1.entrySet()) {
					rows1 +=  column.getValue() + ";";
					
				
				}
				 				rows1 += "\n";
				 				String rows11= rows1.substring(0,rows1.indexOf(";"));
				 				String rows12= rows1.substring(rows1.indexOf(";")+1,rows1.lastIndexOf(";"));
				 				linke.put(Integer.parseInt(rows12), rows11);
				 				rows1="";


			}
			
		}
		
		
		
		for (Entry<Integer, String> entry : linke.entrySet()) 
		{
			gender_val+="'"+entry.getKey()+"',";
			
		}
		gender_val = gender_val.replaceAll(",$", "");
		
		if(linke.size()<10)
		{
			linke=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) ageF).clone();
		}
		
		
		}
	//	System.out.println("COUNT OF GENDER"+linke.size());
			//System.out.println("TESTING"+linke);
		//---------------------------------FILTERING BY OCCUPATION----------------------------------------------
		
		if((linke.size())> 10){
			ageF=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) linke).clone();
			linke.clear();
		
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER) WHERE user.occupation='"+occupation+"' "
								+ "and user.userid IN["+gender_val+"] return user.userid, user.zipcode"))
						
		{	
			while (result.hasNext()) {
				Map<String, Object> row1 = result.next();
			//	Map<String, Object> row2 = new HashMap<>();
				
				for (Map.Entry<String, Object> column : row1.entrySet()) {
					rows1 +=  column.getValue() + ";";
					
				
				}
				 				rows1 += "\n";
				 				String rows11= rows1.substring(0,rows1.indexOf(";"));
				 				String rows12= rows1.substring(rows1.indexOf(";")+1,rows1.lastIndexOf(";"));
				 				linke.put(Integer.parseInt(rows12), rows11);
				 				rows1="";


			}
			
		}
		
		
		
		for (Entry<Integer, String> entry : linke.entrySet()) 
		{
			occupation_val+="'"+entry.getKey()+"',";
			
		}
		occupation_val = occupation_val.replaceAll(",$", "");
		
		if(linke.size()<10)
		{
			linke=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) ageF).clone();
		}
		
		
		}
//		System.out.println("COUNT OF OCCUPATION"+linke.size());
		
		//-------------------------------FILTERING BY ZIPCODE------------------------------------------
		
		if((linke.size())> 10){
		
		System.out.println("\n\n");
			
		for (Entry<Integer, String> entry : linke.entrySet()) 
		{
			String com=zipcode.substring(0,1);
			//System.out.println("compare:"+com);
			String str= entry.getValue().substring(0,1);
			//System.out.println("SubString:"+str);

			if(com.equalsIgnoreCase(str))
			{
				linke1.put(entry.getKey(),entry.getValue());
				
			}
				
			zipcode_val+="'"+entry.getKey()+"',";
			
		}
		zipcode_val = occupation_val.replaceAll(",$", "");
		}

		
	if(linke1.size()<10)
	{
		FinalUsers=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) linke).clone();
	}
	else
	{
		FinalUsers=(Map<Integer, String>) ((LinkedHashMap<Integer, String>) linke1).clone();
	}
	
//	System.out.println("COUNT OF Zipcode"+FinalUsers.size());
		
		//-------------------------------FINISHED FINAL COUNT OF USERS-----------------------------------

	
		String STT="";

			
		
			
			
			 Map<Integer, String> sortedMap = new TreeMap<Integer, String>(FinalUsers);
			 //System.out.println("sortedMap\n");
			int count=0,max=10;
				for (Entry<Integer, String> entry : sortedMap.entrySet()) 
				{
					 if (count >= max) break;
					 {
					USERS.put(entry.getKey(), entry.getKey().toString());	
					 count++;
					 }
				}
							
			//	System.out.println(USERS);
			 
				for (Entry<Integer, String> entry : USERS.entrySet())
				{
					
					STT+="'"+entry.getKey()+"',";
				}
				STT=STT.replaceAll(",$", "");
				//System.out.println(STT);
		//System.out.println("query2 ended");

		
		
		
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
								+ "user.userid IN["+STT+"] AND NOT(item.movieId IN ["+movieId+"])  return DISTINCT item.title  limit 10"))
		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Map.Entry<String, Object> column : row.entrySet()) {
					query +=  column.getValue();
				}
				query += "\n";
			}
		}
		System.out.println(query);
	}

	public void query2(String mid)  {
	
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
		.newEmbeddedDatabase(dbPath);
//ArrayList<String> a = new ArrayList<String>();
Map<Integer, String> GenreCount = new LinkedHashMap<Integer, String>();
Map< String,Integer> MaxMin_Year = new LinkedHashMap<String, Integer>();
Map<Integer, String> MoviCount = new LinkedHashMap<Integer, String>();
String movieID="", action="",thriller="",releaseDate="";
String unknown="", adventure="", animation="", childrens="", comedy="", crime="", documentary="",movies="";
String drama="", fantasy="", filmNoir="", horror="", musical="", mystery="", romance="", sciFi="",  war="", western="";
String max="",min="",countgt="",K="" ,valu="",result1="";	

//-----------------------------------FETCHING MOVIES-----------------------------------------------
	
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid ='"+mid+"'   AND  r.Rating>='4' return  item.movieId ORDER BY  r.Rating DESC"))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			movieID +="'"+column.getValue()+"',";
		}
	}
	
}
movieID=movieID.replaceAll(",$", "");
//System.out.println("MOVIEID's:"+movieID);
	//-----------Unknown--------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.unknown='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			unknown =column.getValue()+"";
		}
		
	}
	
}
unknown=unknown.replaceAll(",$", "");
//System.out.println("unknown:"+unknown);
GenreCount.put(Integer.parseInt(unknown), "unknown");


//-----------Adventure--------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Adventure='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			adventure =column.getValue()+"";
		}
		
	}
	
}
adventure=adventure.replaceAll(",$", "");
//System.out.println("Adventure:"+adventure);
GenreCount.put(Integer.parseInt(adventure), "Adventure");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Animation='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			animation =column.getValue()+"";
		}
		
	}
	
}
animation=animation.replaceAll(",$", "");
//System.out.println("Animation:"+animation);
GenreCount.put(Integer.parseInt(animation), "Animation");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Childrens='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			childrens =column.getValue()+"";
		}
		
	}
	
}
childrens=childrens.replaceAll(",$", "");
//System.out.println("Childrens:"+childrens);
GenreCount.put(Integer.parseInt(childrens), "Childrens");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Comedy='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			comedy =column.getValue()+"";
		}
		
	}
	
}
comedy=comedy.replaceAll(",$", "");
//System.out.println("Comedy:"+comedy);
GenreCount.put(Integer.parseInt(comedy), "Comedy");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Crime='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			crime =column.getValue()+"";
		}
		
	}
	
}
crime=crime.replaceAll(",$", "");
//System.out.println("Crime:"+crime);
GenreCount.put(Integer.parseInt(crime), "Crime");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Documentary='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			documentary =column.getValue()+"";
		}
		
	}
	
}
documentary=documentary.replaceAll(",$", "");
//System.out.println("Documentary:"+documentary);
GenreCount.put(Integer.parseInt(documentary), "Documentary");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Drama='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			drama =column.getValue()+"";
		}
		
	}
	
}
drama=drama.replaceAll(",$", "");
//System.out.println("Drama:"+drama);
GenreCount.put(Integer.parseInt(drama), "Drama");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Fantasy='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			fantasy =column.getValue()+"";
		}
		
	}
	
}
fantasy=fantasy.replaceAll(",$", "");
//System.out.println("Fantasy:"+fantasy);
GenreCount.put(Integer.parseInt(fantasy), "Fantasy");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.FilmNoir='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			filmNoir =column.getValue()+"";
		}
		
	}
	
}
filmNoir=filmNoir.replaceAll(",$", "");
//System.out.println("FilmNoir:"+filmNoir);
GenreCount.put(Integer.parseInt(filmNoir), "FilmNoir");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Horror='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			horror =column.getValue()+"";
		}
		
	}
	
}
horror=horror.replaceAll(",$", "");
//System.out.println("Horror:"+horror);
GenreCount.put(Integer.parseInt(horror), "Horror");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Musical='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			musical =column.getValue()+"";
		}
		
	}
	
}
musical=musical.replaceAll(",$", "");
//System.out.println("Musical:"+musical);
GenreCount.put(Integer.parseInt(musical), "Musical");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Mystery='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			mystery =column.getValue()+"";
		}
		
	}
	
}
mystery=mystery.replaceAll(",$", "");
//System.out.println("Mystery:"+mystery);
GenreCount.put(Integer.parseInt(mystery), "Mystery");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Romance='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			romance =column.getValue()+"";
		}
		
	}
	
}
romance=romance.replaceAll(",$", "");
//System.out.println("Romance:"+romance);
GenreCount.put(Integer.parseInt(romance), "Romance");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.SciFi='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			sciFi =column.getValue()+"";
		}
		
	}
	
}
sciFi=sciFi.replaceAll(",$", "");
//System.out.println("SciFi:"+sciFi);
GenreCount.put(Integer.parseInt(sciFi), "SciFi");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Thriller='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			thriller =column.getValue()+"";
		}
		
	}
	
}
thriller=thriller.replaceAll(",$", "");
//System.out.println("Thriller:"+thriller);
GenreCount.put(Integer.parseInt(thriller), "Thriller");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.War='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			war =column.getValue()+"";
		}
		
	}
	
}
war=war.replaceAll(",$", "");
//System.out.println("War:"+war);
GenreCount.put(Integer.parseInt(war), "War");
//-----------------
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Western='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			western =column.getValue()+"";
		}
		
	}
	
}
western=western.replaceAll(",$", "");
//System.out.println("Western:"+western);
GenreCount.put(Integer.parseInt(western), "Western");
//------------------------------------FETCHING THE COUNT OF ACTION------------------------------------

try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.movieId IN ["+movieID+"] AND item.Action='1' return   COUNT(user.userid) "))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			action =column.getValue()+"";
		}
		//movieId += "\n";
	}
	
}
action=action.replaceAll(",$", "");
//System.out.println("Action:"+action);
GenreCount.put(Integer.parseInt(action), "Action");
//System.out.println("Genre"+GenreCount);
Map<Integer, String> sortedMap1 = new TreeMap< Integer, String>(GenreCount);
//System.out.println("sortedMap1"+sortedMap1);
String value="";
for (Entry<Integer,String> entry : sortedMap1.entrySet())
{
	value=entry.getValue();
}

System.out.println("Genre With Maximum Count:"+value);
//-----------------------------------------YEAR CALCULATION-------------------------
String ss="";
try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid ='"+mid+"'  return  item.videoReleaseDate "))

{ int count=0;
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			//video +=""+column.getValue()+"";
			 ss+= (String) column.getValue();
			 String sub   =(String)column.getValue();
			 MaxMin_Year.put(sub,count);
			 count++;
			 }
		ss+="\n";
	
	}
	
}

//System.out.println("YEAR:"+ss);




//System.out.println("RELEASE:"+releaseDate);
Map<String, Integer> sortedMap = new TreeMap< String,Integer>(MaxMin_Year);
min = sortedMap.keySet().iterator().next();
//System.out.println("MINYEAR:"+min);

/*
String string_date = "05/04/99";

SimpleDateFormat f = new SimpleDateFormat("MM/dd/yy");
Date d = null;
try {
	d = f.parse(string_date);
} catch (ParseException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
long milliseconds = d.getTime();
System.out.println("MILLI"+milliseconds);*/

for (Entry<String, Integer> entry : sortedMap.entrySet())
{
	
	max=entry.getKey();
}
//System.out.println("MAXYEAR"+max);

Integer max_val=Integer.parseInt(max);
Integer min_val=Integer.parseInt(min);

	Integer difference = max_val-min_val;
//	System.out.println("Diff"+difference);
	String a="01/01/";
	String b="12/31/";
	String Startdate="";
	String Enddate="";
if(difference<10)
{
	Startdate= a+min;
	Enddate= b+max;
	System.out.println(Startdate);
	System.out.println(Enddate);
	

	try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.ReleaseDate>='"+Startdate+"' AND item.ReleaseDate <='"+Enddate+"' AND item."+value+"='1' return  item.title  ORDER BY item.movieId DESC LIMIT 10"))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			movies +=column.getValue()+"";
		}
		movies += "\n";
	}
	
}
movies=movies.replaceAll(",$", "");
//System.out.println("movies:"+movies);


	
	
	
}
else if (difference>=10 && difference <=30)
{
	Integer Start_date=min_val;
	
	Startdate=a.concat(Start_date.toString());
	//System.out.println("Startdate"+Startdate+"::min_val"+min_val+"::");
	Integer End_date=min_val+5;
	Enddate=b.concat(End_date.toString());
	//System.out.println("Enddte"+Enddate+"::max_val"+max_val+"::End_date:"+End_date);
	while(End_date <max_val)
	{
		
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
								+ "user.userid  IN['"+mid+"']  AND item.ReleaseDate>='"+Startdate+"' AND item.ReleaseDate <='"+Enddate+"'   return  COUNT(item.ReleaseDate) "))

		{
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Map.Entry<String, Object> column : row.entrySet()) {
					countgt =column.getValue()+"";
					 K=  countgt;
					String val = Startdate+":"+Enddate;
					MoviCount.put(Integer.parseInt(K), val);
				}
			
			}
			
		}
		
		min_val=min_val+5;
		Start_date=min_val;
		Startdate=a.concat(Start_date.toString());
		End_date=Start_date+5;
		Enddate=b.concat(End_date.toString());
		
		
	}
}
	else if (difference>30 )
	{
		Integer Start_date=min_val;
		
		Startdate=a.concat(Start_date.toString());
		System.out.println("Startdate"+Startdate+"::min_val"+min_val+"::");
		Integer End_date=min_val+10;
		Enddate=b.concat(End_date.toString());
		System.out.println("Enddte"+Enddate+"::max_val"+max_val+"::End_date:"+End_date);
		while(End_date <max_val)
		{
			
			try (Transaction tx = graphDb.beginTx();
					Result result = graphDb
							.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
									+ "user.userid  IN['"+mid+"']  AND item.ReleaseDate>='"+Startdate+"' AND item.ReleaseDate <='"+Enddate+"'  return  COUNT(item.ReleaseDate) "))

			{
				while (result.hasNext()) {
					Map<String, Object> row = result.next();
					for (Map.Entry<String, Object> column : row.entrySet()) {
						countgt =column.getValue()+"";
						 K=  countgt;
						String val = Startdate+":"+Enddate;
						MoviCount.put(Integer.parseInt(K), val);
					}
				
				}
				
			}
			
			min_val=min_val+10;
			Start_date=min_val;
			Startdate=a.concat(Start_date.toString());
			End_date=Start_date+10;
			Enddate=b.concat(End_date.toString());
			
			
		}
	System.out.println("movies:"+countgt);
	System.out.println(MoviCount);
	
	}
Map<Integer, String> sortedMap2 = new TreeMap<Integer, String>(MoviCount);
//System.out.println("M2"+sortedMap2);
for(Map.Entry<Integer,String> column : sortedMap2.entrySet())
{
	valu=column.getValue();
	
}


String p=valu.substring(0,valu.indexOf(":"));
String q=valu.substring(valu.indexOf(":"), valu.length());


try (Transaction tx = graphDb.beginTx();
		Result result = graphDb
				.execute("match (user:USER)-[r:HAS_ITEM]->(item:ITEM) WHERE "
						+ "user.userid  IN['"+mid+"']  AND item.ReleaseDate>='"+Startdate+"' AND item.ReleaseDate <='"+Enddate+"' AND item."+value+"='1' return  item.title  ORDER BY item.movieId DESC LIMIT 10"))

{
	while (result.hasNext()) {
		Map<String, Object> row = result.next();
		for (Map.Entry<String, Object> column : row.entrySet()) {
			result1+= column.getValue()+"";
		
		}
		result1+="\n";
	}
	
}
System.out.println(result1);
	
	}
}