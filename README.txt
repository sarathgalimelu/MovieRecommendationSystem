Data Model used for SQL:

Several SQL tables are created from the data files. All tables are associated with primary and foreign key relations.

•	A table for occupation is created, with only occupation field

CREATE TABLE occupation
(
Occupation varchar(255) NOT NULL,
PRIMARY KEY (occupation)
);


•	A user table is created with columns – user id, age, gender, occupation and zip code

CREATE TABLE user
(
userid int NOT NULL,
age varchar(255) NOT NULL,
gender varchar(255),
occupation varchar(255),
zipcode  varchar(255),
PRIMARY KEY (userid),
FOREIGN KEY (occupation)
REFERENCES occupation(occupation)
);

•	A table for genre is created with genre and serial number.

CREATE TABLE genre
(
sno int NOT NULL,
genre varchar(255),
PRIMARY KEY (genre)
);
•	A table for item with all the movie details is created. It has movie id, movie title, release date and IMDBurl.

CREATE TABLE item
(
movieid int NOT NULL,
movietitle varchar(255) NOT NULL,
videoreleasedate date,
IMDBurl varchar(255),
PRIMARY KEY (movieid)
);

•	A movie-genre table is created with movieid’s and their corresponding genres.

CREATE TABLE moviegenre
(
movieid int NOT NULL,
genre varchar(255) NOT NULL,
FOREIGN KEY (movieid)
REFERENCES item(movieid),
FOREIGN KEY (genre)
REFERENCES genre(genre),
PRIMARY KEY(movieid,genre)
);

•	A data table is created with all the movie id’s, relevant itemid’s, rating and timestamp.

CREATE TABLE data
(
userid int NOT NULL,
itemid int NOT NULL,
rating int NOT NULL,
timestamp INT(11) NOT NULL,
FOREIGN KEY (userid)
REFERENCES user(userid),
FOREIGN KEY (itemid)
REFERENCES item(movieid),
PRIMARY KEY(userid,itemid)
);




Data Model for Neo 4j:

1.	First we create nodes in neo 4j also know as Labels, we create relationship between these labels.
2.	Our data set consist of movies and users, and we establish a relationship between them – “HAS_ITEM”.
3.	While creating relationship in the “data” data set, we pull out movies from item data set and users from user data set, ranking and timestamp from “data” data set.
4.	This establishes a relationship between data file, item file and user file, which is the crux of this data set to operate.
5.	By having this set up, we are ready with the data model of neo 4j




Ranking Criteria of Algorithms:

Collaborative Filtering:

1.	In this algorithm, we are first retrieving age, gender, occupation and zip code of the given user id.
2.	In the next steps, we select the users who have given rating equal to the input rating to the movies rated by input user.
3.	Now we have the list of the users similar to input user id.
4.	We have not given weights to age, gender, occupation and zip code. Instead we have filtered users in this order age> gender> occupation>zip code. 
5.	And after every iteration, we check if the number of users >10, because we want only 1% of 943 users. If the count falls below 10, we would take the number of users from the previous iteration. There may be a case, final users are only in same age group and gender, may not pursue same occupation and have same zip code. This shows that age, gender given high priority over occupation and zip code.
6.	As explained, we would now get list of users who fall in the same age (age-5, age+5) range as input user.
7.	In this user list, we will now pull list of users who are of same gender.
8.	After, we get users of same occupation.
9.	And finally we take users who have same first digit in their zip codes, i.e. users of Georgia will have zip code that starts with ‘6’
10.	Once we pull out users based on age, gender, occupation and zip code in that order.
11.	If you in the above steps, we have been ensuring in every iteration that the number of users doesn’t fall below 10 users (top 1% of 943 users).
12.	If in case, after some iteration we get users less than 10 then we take the count from the previous iteration.
13.	But even if the count of previous iteration users greater than 10, then we take top 10 of this list by ordering them.
14.	Finally we in most cases get 10 users which share most similarities with the input user.
15.	 Using these user ids, we now get movies list which are not rated by input user.
16.	 In those movies, we take top 10 movies by ordering them.

Collaborative Based Filtering Algorithm working with example:
•	Let us run this algorithm on a user id and rating.
•	Initial look of the run, looks as below:
Welcome Enter your query choice 
1 collaborative filtering
2 item-based filtering
3 quit
Enter your choice:
1
Enter user id:
123
Enter minimum rating:
4
•	Now we have 123 as user id and 4 as rating for this example.
•	First we get age, gender, occupation and zip code of this user, which is  as below:

Age: 48

Gender: F

Occupation: artist

Zip code: 20008

•	After that, we get list of users who has given rating of 4 for the same movies rated by user id - 123. 
•	We got 597 users who fall in this range, from this list we pull out users in the same age group (43-53 age groups).

After age iteration: 114 users

•	Now we check for users with same gender, we got below count

After gender iteration: 38 users

•	Now check for users with same occupation.

After occupation iteration: 2 users

•	As we got less than 10 users, we go to previous iteration and get those 38 users, and also we stop the iterations, we don’t check for zip code similarity. User ids of those 38 users:

908,2,942,704,835,720,722,607,623,503,629,878,518,401,529,655,535,539,799,316,438,321,444,204,693,463,225,236,15,123,389,273,65,296,72,185,89,902

•	As we see, we got users with same age and genders only, as they are given high priority.
•	From these user ids, we take top 10 movies by ordering them. Below is the movie list:

Toy Story (1995)
GoldenEye (1995)
Copycat (1995)
Twelve Monkeys (1995)
Dead Man Walking (1995)
Richard III (1995)
Usual Suspects, The (1995)
Mighty Aphrodite (1995)
Postino, Il (1994)
Mr. Holland's Opus (1995)


Item Based Filtering:
1.	In this algorithm, we filter movies list by items such as genre and release year.
2.	Firstly, we take list of movies highly rated by input user.
3.	In this list, we find the genre which is rated most highly by input user for most number of times.
4.	After this step, we get top genre preferred by input user.
5.	Now, we find the year range in which input user has rated for most number of times.
6.	To do this, we split the range of years in which input user has rated.
7.	If the difference in years is <10, then we take this range to find final movie list.
8.	But if the difference in years is >10 and <30, then we split the year range into 5 years, and check in each range the count of movies rated by input user.
9.	After we get all counts for the corresponding ranges, we then pick the range which has highest count. This highest count means, the input user likes the movies in that year range.
10.	Also if the difference in years is >30, we repeat step 8 and 9, but we split the range into sub ranges of 10 years each.
11.	After all this, we got genre highly watched by input and year range in which the input user has watched more number of movies.
12.	Using these 2 parameters, we get the movies list. This movies list does not have movies rated by him already.
13.	From this movie list, we take top 10 by ordering the movie list.

Item Based Filtering Algorithm working with example:
•	The execution starts as below:
Welcome.. Enter your query choice 
1 collaborative filtering
2 item-based filtering
3 quit
Enter your choice:
2
Enter user id:
234
•	In the first step, we get genre highly rated by input user (user id - 2) and rated most number of times as below:
Drama - 83
Comedy – 43
•	Now we have Drama and Comedy as most watched by user (user id - 234).
•	After we get drama, we check the initial and final year of his rating history.
This is the initial date: 1922-01-01
This is the final date: 1997-12-23

•	As the difference between them is >30, we split into ranges of 10 years, and check the number of movies rated in those ranges as shown below:
75
1922-01-01/1932-12-31
4
1932-01-01/1942-12-31
42
1942-01-01/1952-12-31
34
1952-01-01/1962-12-31
48
1962-01-01/1972-12-31
33
1972-01-01/1982-12-31
39
1982-01-01/1992-12-31
41
1992-01-01/1997-12-31
95
•	Now we can see that, in range 92-97, he has rated maximum movies – 95. Hence year range is 92-97.
•	After we get time range, we have genre already, we pull out movies of this genre in this range.
•	In this movie list, we pull out top 10 movies not watched by user (user id - 234) by ordering them. Below is the top 10 movie list.

Kundun (1997)
As Good As It Gets (1997)
Amistad (1997)
Career Girls (1997)
Contact (1997)
Pillow Book, The (1995)
Brassed Off (1996)
Love! Valour! Compassion! (1997)
Quiet Room, The (1996)
Selena (1997)


Comparison of performances of Neo4j and SQL:

SQL Performance:

	Loading Time – 20.222 seconds
Collaborative Filtering Recommendation time – 1.217 seconds
	Item Based Filtering – 0.654 seconds
	
Neo 4j Performance:

	Loading Time – 4.169 seconds
Collaborative Filtering Recommendation time – 4.503 seconds
	Item Based Filtering – 3.484 seconds
