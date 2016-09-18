import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;




public class MySQLRecommend {


	
	public static void main(String args[]) {

	
		
		
		Scanner scanner = new Scanner(System.in);
		int choice = 0;
		System.out
				.println("Welcome.. Enter your query choice \n1 collaborative filtering\n2 item-based filtering\n3 quit");
		System.out.println("Enter your choice:");
		do {

			choice = scanner.nextInt();
			if (choice == 1){
				System.out.println("Enter user id:");
			int uid = scanner.nextInt();
			System.out.println("Enter minimum rating:");
			int rating = scanner.nextInt();
			if((uid>0 && uid<944)){
				if(rating>0 && rating<=5){
					long startTime = System.currentTimeMillis();
				collaborativeFilering(uid,rating);
				long endTime = System.currentTimeMillis();
				long totalTime = endTime - startTime;
				System.out.println("total execution time"+totalTime);
				
				}
				else
				{
					System.out.println("Wrong choice.. Try again");
				}
			}
			else
			{
				System.out.println("Wrong choice.. Try again");
				
			}
			}
			else if (choice == 2){
				System.out.println("Enter user id:");
				int uid = scanner.nextInt();
				if(uid>0 && uid<944){
					long startTime = System.currentTimeMillis();
					itemBasedFilering(uid);
					long endTime = System.currentTimeMillis();
					long totalTime = endTime - startTime;
					System.out.println("total execution time"+totalTime);
				
				}
				else
				{
					System.out.println("Wrong choice.. Try again");
					System.out
							.println("Welcome.. Enter your query choice \n1 collaborative filtering\n2 item-based filtering\n3 quit");
				}
			}
			else if (choice == 3) {
				System.out.println("bye..");
				System.exit(0);
			} else
				System.out.println("Wrong choice.. Try again");
			System.out
					.println("Welcome.. Enter your query choice \n1 collaborative filtering\n2 item-based filtering\n3 quit");
			
			
		} while (choice != 3);
		scanner.close();

	}
	
	  public static Connection connection(){
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
			String dbname = prop.getProperty("dbname");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
		  
		  String myDriver = "com.mysql.jdbc.Driver";
	      String myUrl = "jdbc:mysql://localhost:3306/"+dbname;
		  //String myUrl = username;
	      Connection conn = null;
	      try {
			Class.forName(myDriver);
			conn = DriverManager.getConnection(myUrl, username, password);
			
		} catch (ClassNotFoundException | SQLException e) {
			
			e.printStackTrace();
		}
	      
		  return conn;
		  
	  }
	
	public static void itemBasedFilering(int uid){
		
		Connection con = connection();
		//String query = "select data.itemid,data.rating,item.videoreleasedate,moviegenre.genre from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" order by data.rating desc";
		String query = "select count(moviegenre.genre),moviegenre.genre,data.rating,item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" group by moviegenre.genre order by count(moviegenre.genre) desc,data.rating desc limit 2";
		//String query = "select count(moviegenre.genre),moviegenre.genre,data.rating,item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" group by moviegenre.genre order by count(moviegenre.genre) desc,data.rating desc";
		Statement st;
		String twogenres = null;
		try {
			st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		while(rs.next()){
			String genre1 = rs.getString("genre");
			String count = rs.getString("count(moviegenre.genre)");
			if(twogenres == null){
			twogenres = genre1;}
			else{
				twogenres=twogenres+","+genre1;
			}
			//System.out.println(genre1+"add"+count);
			//Date release = rs.getDate("videoreleasedate");
			//System.out.println(release);
		}
		//System.out.println(twogenres);
		
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		//System.exit(0);
		String querymin = "select item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and item.videoreleasedate= (select item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" order by item.videoreleasedate asc limit 1) limit 1";
		String querymax = "select item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and item.videoreleasedate= (select item.videoreleasedate from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" order by item.videoreleasedate desc limit 1) limit 1";
		String initial = null;
		String fina = null;
		String finalrange = null;
		
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(querymin);
			
			while(rs1.next()){
			Date release = rs1.getDate("videoreleasedate");
			Format formatter = new SimpleDateFormat("yyyy-MM-dd");
			String s = formatter.format(release);
			
			initial = s.split("-")[0];
			//System.out.println("this is the initial date"+release);
			}
			}
		catch(SQLException e){
			e.printStackTrace();
		}
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(querymax);
			
			while(rs1.next()){
			Date release = rs1.getDate("videoreleasedate");
			Format formatter = new SimpleDateFormat("yyyy-MM-dd");
			String s = formatter.format(release);
			fina = s.split("-")[0];
			//System.out.println("this is the final date"+release);
		}
			}
		catch(SQLException e){
			e.printStackTrace();
		}
		int diff = Integer.parseInt(fina)-Integer.parseInt(initial);
		//System.out.println(diff);
		
		Map<String, Integer> hmap = new HashMap<String, Integer>();
		if(diff<=10){
			String ini = initial;
			String dat = ini+"-01-01";
			int dattmp = Integer.parseInt(initial);
			int fintmp = Integer.parseInt(fina);
			String datnext =fina+"-12-31";
			finalrange = dat+","+datnext;
		}
	
		if(diff>10 && diff<30){
			
			String ini = initial;
			String dat = ini+"-01-01";
			int dattmp = Integer.parseInt(initial);
			int fintmp = Integer.parseInt(fina);
			int datinc = dattmp+5;
			String datnext =datinc+"-12-31";
			
			while(datinc<=fintmp)
			{
				//System.out.println(dat+datnext);
				String querysplit = "select count(item.videoreleasedate) from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and videoreleasedate between '"+dat+"' and '"+datnext+"'";
				try{
					st = con.createStatement();
					ResultSet rs1 = st.executeQuery(querysplit);
					
					while(rs1.next()){
						//System.out.println("hello");
						int count = rs1.getInt("count(item.videoreleasedate)");
						hmap.put(dat+","+datnext,count);
						//System.out.println("count is"+count);
					}
				}
					catch(SQLException e){
						
						
					}
				datinc = datinc+5;
				datnext =datinc+"-12-31";
				dattmp = dattmp+5;
				dat = dattmp+"-01-01";

			}
			if(datinc>fintmp){
			String findat = fintmp+"-12-31";
			int temo = datinc-5;
			String lastdat = temo+"-01-01";
		String querysplit1 = "select count(item.videoreleasedate) from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and videoreleasedate between '"+lastdat+"' and '"+findat+"'";
		//System.out.println(lastdat);
		//System.out.println(findat);
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(querysplit1);
			
			while(rs1.next()){
				int count = rs1.getInt("count(item.videoreleasedate)");
				hmap.put(lastdat+","+findat,count);
				//System.out.println("96-97"+count);
			}
		}
			catch(SQLException e){
				
			}
			
			}
			Map.Entry<String, Integer> maxEntry = null;

			for (Map.Entry<String, Integer> entry : hmap.entrySet())
			{
			    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
			    {
			        maxEntry = entry;
			      
			   
			    }
			}
			if(maxEntry!=null){ 
			finalrange = maxEntry.getKey();
			  //System.out.println(finalrange);
			}
			 // hmap.clear();
			
		}
		
		if(diff>30){
		
			String ini = initial;
			String dat = ini+"-01-01";
			int dattmp = Integer.parseInt(initial);
			int fintmp = Integer.parseInt(fina);
			int datinc = dattmp+10;
			String datnext =datinc+"-12-31";
			//System.out.println(dat+"/"+datnext);
			while(datinc<=fintmp)
			{
				//System.out.println(datinc);
				//System.out.println(fintmp);
				//System.out.println(dat+"/"+datnext);
				//System.out.println(uid);
				String querysplit = "select count(item.videoreleasedate) from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and videoreleasedate between '"+dat+"' and '"+datnext+"'";
				try{
					st = con.createStatement();
					ResultSet rs1 = st.executeQuery(querysplit);
					
					while(rs1.next()){
						int count = rs1.getInt("count(item.videoreleasedate)");
						hmap.put(dat+","+datnext,count);
						//System.out.println(dat+"/"+datnext);
						//System.out.println(count);
					}
				}
					catch(SQLException e){
						
					}
				
				datinc = datinc+10;
				datnext =datinc+"-12-31";
				dattmp = dattmp+10;
				dat = dattmp+"-01-01";

			}
			if(datinc>fintmp){
				String findat = fintmp+"-12-31";
				int temo = datinc-10;
				String lastdat = temo+"-01-01";
			String querysplit1 = "select count(item.videoreleasedate) from data,item,moviegenre where data.itemid = item.movieid and moviegenre.movieid=item.movieid and data.rating>=4 and userid="+uid+" and videoreleasedate between '"+lastdat+"' and '"+findat+"'";
			//System.out.println(lastdat);
			//System.out.println(findat);
			
			try{
				st = con.createStatement();
				ResultSet rs1 = st.executeQuery(querysplit1);
				
				while(rs1.next()){
					int count = rs1.getInt("count(item.videoreleasedate)");
					//System.out.println(lastdat+","+findat);
					hmap.put(lastdat+","+findat,count);
					//System.out.println(count);
					//System.out.println("96-97"+count);
				}
			}
				catch(SQLException e){
					
				}
			}
			//System.out.println(dat+"/"+datnext);
			//System.out.println("50");
			
			Map.Entry<String, Integer> maxEntry = null;

			for (Map.Entry<String, Integer> entry : hmap.entrySet())
			{
			    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
			    {
			        maxEntry = entry;
			      
			   
			    }
			}
			if(maxEntry!=null){ 
			  finalrange = maxEntry.getKey();
			}
			hmap.clear();
		}
		
		String aa = finalrange+twogenres;
		//System.out.println(aa);
		
		String [] partdate = finalrange.split(",");
		String [] partgenre = twogenres.split(",");
		String querysplit1 = "select distinct item.movietitle from item,moviegenre,data where moviegenre.movieid=item.movieid and data.itemid=item.movieid and videoreleasedate between '"+partdate[0]+"' and '"+partdate[1]+"' and moviegenre.genre='"+partgenre[0]+"' order by data.rating desc,item.videoreleasedate desc limit 10";
		//String querysplit1 = "select distinct item.movietitle from item,moviegenre where moviegenre.movieid=item.movieid and videoreleasedate between '"+partdate[0]+"' and '"+partdate[1]+"' and moviegenre.genre='"+partgenre[0]+"' order by item.movieid desc limit 10";
		
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(querysplit1);
			
			while(rs1.next()){
				//System.out.println("hello");
				String title = rs1.getString("movietitle");
				System.out.println(title);
			}
		}
			catch(SQLException e){
				
			}
	}

	public static void collaborativeFilering(int uid, int rating){
		
		Connection con = connection();
		String querysplit1 = "select age,gender,occupation,zipcode from user where userid="+uid;
		List<String> finalList = new ArrayList<String>();
		
		String allusermov = null;
		
		Statement st = null;
		String age = null;
		String gender = null;
		String occupation = null;
		String zipcode = null;
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(querysplit1);
			
			while(rs1.next()){
				
				age = rs1.getString("age");
				gender = rs1.getString("gender");
				occupation = rs1.getString("occupation");
				zipcode = rs1.getString("zipcode");
			
				//System.out.println("age"+age+"\n");
				//System.out.println("Gender"+gender+"\n");
				//System.out.println("Occupation"+occupation+"\n");
				//System.out.println("zipcode"+zipcode+"\n");
			}
		}
			catch(SQLException e){
				
			}
		
		List<String> allmovies = new ArrayList<String>();
		String movsql= "select itemid from data where userid="+uid+" and rating="+rating;
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(movsql);
			
			while(rs1.next()){
				
				int movieid = rs1.getInt("itemid");
				String movesid = ""+movieid;
				allmovies.add(movesid);
			}
		}
			catch(SQLException e){
				
			}
		
	for(int mn = 0;mn<allmovies.size();mn++){
		if(allusermov==null){
			allusermov = allmovies.get(mn);
			}
			else
			{
				
				allusermov =allusermov + ","+allmovies.get(mn);
			}
			
			//System.out.print("ichinausermovies"+allusermov);
		}
	//System.out.println(allmovies.size());
	
	Set<String> hs = new HashSet<>();
		List<String> userids = new ArrayList<String>();
		for(int sup=0;sup<allmovies.size();sup++){
		String movuser= "select distinct(userid) from data where itemid="+allmovies.get(sup)+" and rating="+rating;
		try{
			st = con.createStatement();
			ResultSet rs2 = st.executeQuery(movuser);
			
			while(rs2.next()){
				
				Integer useridsfin = rs2.getInt("userid");
				String useridstr = useridsfin.toString();
				userids.add(useridstr);
			}
		}
			catch(SQLException e){
				
			}
		}
		
		hs.addAll(userids);
		userids.clear();
		userids.addAll(hs);
		
		for(int mn = 0;mn<userids.size();mn++){
			
			//System.out.println("userids"+userids.get(mn)+",");
		}
		//System.out.println("num of users"+userids.size());

		//System.exit(0);
		
		int agecon = Integer.parseInt(age);
		int age1 = agecon-5;int age6 = agecon+1;
		int age2 = agecon-4;int age7 = agecon+2;
		int age3 = agecon-3;int age8 = agecon+3;
		int age4 = agecon-2;int age9 = agecon+4;
		int age5 = agecon-1;int age10 = agecon+5;
		
		//pulling similar age people
		String agelist = age1+","+age2+","+age3+","+age4+","+age5+","+agecon+","+age6+","
						+age7+","+age8+","+age9+","+age10;
		//System.out.println(agelist);
		int i=0;
		List<String> myList = new ArrayList<String>();
		for(int lp=0;lp<userids.size();lp++){
		String agequery = "select userid from user where age in("+agelist+") and userid="+userids.get(lp);
		
		try{
			st = con.createStatement();
			ResultSet rs1 = st.executeQuery(agequery);
			
			while(rs1.next()){
				
				int agesgrpid = rs1.getInt("userid");
				String agesss = ""+agesgrpid;
				i++;
				
				myList.add(agesss);
				//System.out.println(agesgrpid);
			}
		}
			catch(SQLException e){
				
			}
	
		}
		//System.out.println("after age"+i);
		//System.out.println(gender);
		int k=0;
		int jumcase = 0;
		List<String> genderList = new ArrayList<String>();
		if(i>10){
			
			
			for(int j=0;j<myList.size();j++){
			String genderquery = "select userid from user where userid="+myList.get(j)+" and gender='"+gender+"'";
			//System.out.println("userid value:"+myList.get(j));
			try{
				st = con.createStatement();
				ResultSet rs1 = st.executeQuery(genderquery);
				
				while(rs1.next()){
					
					int userid = rs1.getInt("userid");
					String tempuser = ""+userid;
					k++;
					genderList.add(tempuser);
					//System.out.println(userid);
				}
			}
				catch(SQLException e){
					
				}
			
			}	
		}
		else{
			jumcase=1;
			finalList = myList;
			
		}
		
		int m=0;
		//System.out.println("values after gender"+k);
		List<String> occList = new ArrayList<String>();
		List<String> ziplist = new ArrayList<String>();
		int genderjump = 0;
		if(jumcase!=1){
		if(k>10){
			//System.out.println("into occupations");
				for(int l =0; l<genderList.size(); l++){
					

					String occquery = "select userid,zipcode from user where userid="+genderList.get(l)+" and occupation='"+occupation+"'";
					//System.out.println("userid value:"+genderList.get(l));
					try{
						st = con.createStatement();
						ResultSet rs1 = st.executeQuery(occquery);
						
						while(rs1.next()){
							
							int userid = rs1.getInt("userid");
							String zip = rs1.getString("zipcode");
							String tempuser = ""+userid;
							m++;
							occList.add(tempuser);
							ziplist.add(zip);
							//System.out.println("occupation user id"+userid);
						}
					}
						catch(SQLException e){
							
						}
				}
			
		}
		else{
			genderjump=1;
			finalList = myList;
		}
		}
		//System.out.println("after occupation"+ziplist.size());
		String zipfirst = zipcode.substring(0,1);
		int p = 0;
		int asaljump = 0;
		List<String> afterziplist = new ArrayList<String>();
		if(genderjump!=1){
				if(m>10){
					
					for(int n =0; n<ziplist.size(); n++){
						//System.out.println(ziplist.get(n));
					String first = ziplist.get(n).substring(0,1);
					//System.out.println(first+"samenot"+zipfirst);
					//Boolean b = first.equalsIgnoreCase(zipfirst);
					//System.out.println(b);
					if(first.equalsIgnoreCase(zipfirst)){
						//System.out.println("hello");
						afterziplist.add(occList.get(n));
						p++;
					}			
				}
		}
				else{
					//System.out.println("into zip hadaaa");
					//finalList.clear();
					asaljump=1;
					finalList = genderList;
				}
		}
		if(asaljump!=1){
		if(p>10){
			
			finalList = afterziplist;
			
		}
		else{
			finalList=occList;
			
		}
		}
			//System.out.println("final after zip"+p);
			//System.out.println("size of final"+finalList.size());
			for(int n=0; n<finalList.size();n++){
				//System.out.println("final ids"+finalList.get(n));
				
				}
			
			
			List<String> finalthopu = new ArrayList<String>();
			String comlist = null;
			if(finalList.size()>10){
			//System.out.println("hello");
				String finass = null;
				for(int j=0;j<finalList.size();j++){
					if(finass==null){
					finass = finalList.get(j);
					}
					else
					{
						
						finass = finass + ","+finalList.get(j);
					}
					
				}
				//System.out.println(finass);
				
					
					String genderquery = "select userid from user where userid in ("+finass+") order by userid limit 10";
					
					try{
						st = con.createStatement();
						ResultSet rs1 = st.executeQuery(genderquery);
						
						while(rs1.next()){
							
							int userid = rs1.getInt("userid");
							String tempuser = ""+userid;
							k++;
							finalthopu.add(tempuser);
							//System.out.println(userid);
						}
					}
						catch(SQLException e){
							
						}
					
						for(int mn = 0; mn<finalthopu.size();mn++){
						
						//System.out.println("userids -10- "+finalthopu.get(mn));
						if(comlist==null){
							comlist = finalthopu.get(mn);
							}
							else
							{
								
								comlist = comlist + ","+finalthopu.get(mn);
							}
							
							}
			
				}
			//System.out.println(comlist);
			
			String idhelastquery = "select distinct movietitle from item,data where item.movieid = data.itemid and data.userid in ("+comlist+") and data.itemid not in ("+allusermov+") order by item.movieid limit 10";
			List<String> idheelasts = new ArrayList<String>();
			try{
				st = con.createStatement();
				ResultSet rs1 = st.executeQuery(idhelastquery);
				
				while(rs1.next()){
					
					String movietitle = rs1.getString("movietitle");
					String tempuser = ""+movietitle;
					
					idheelasts.add(tempuser);
					System.out.println(movietitle);
				}
			}
				catch(SQLException e){
					
				}
				
	
	}
	
	
	
}