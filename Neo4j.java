package Neo4jHW.graphNeo4j;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.*;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import org.neo4j.index.impl.lucene.legacy.*;
import org.neo4j.graphdb.Transaction;
import static org.neo4j.driver.v1.Values.parameters;
class Actors{
	Integer id;
	String first_name;
	String last_name;
	public Actors(Integer id, String first_name, String last_name) {
		super();
		this.id = id;
		this.first_name = first_name;
		this.last_name = last_name;
	}
	
	
}
class roles{
	String roles;
	int movie_id;
	roles(String role, int id){
		this.roles=role;
		this.movie_id=id;
	}
}

class Directors{
	Integer id;
	String first_name;
	String last_name;
	public Directors(Integer id, String first_name, String last_name) {
		super();
		this.id = id;
		this.first_name = first_name;
		this.last_name = last_name;
	}	
		
	}

class Genre{
	String genres;

	public Genre(String genres) {
		super();
		this.genres = genres;
	}
	
}
class Movies{
	Integer id;
	String title;
	int year;
	public Movies(Integer id, String title, int year) {
		super();
		this.id = id;
		this.title = title;
		this.year = year;
	}
	
}

public class Neo4j {
	static final String user="root";
	static final String password="Shubhu06081996";
	static final String url="jdbc:mysql://localhost:3306/homework1";
	static final String jdbc="com.mysql.jdbc.Driver";
	static final String neo4j="bolt://localhost";
	static final File DB_PATH = new File( "C:/Users/gayat/OneDrive/Documents/Neo4j/IMDBNEW1" );
	
	public static void main(String args[]){
		try{
			//creates connection with MySQL
			Class.forName(jdbc);  
			Connection con=null;
			Connection con1=null;
			Statement stmt=null;
			
			con=DriverManager.getConnection(url,user,password);
			stmt=con.createStatement();  
			System.out.println("connection done");
			//List<BasicDBObject> moviesList = new ArrayList<BasicDBObject>();
			
			stmt = con.createStatement();
			
			
			System.out.println("connection2 is done");
			GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
			GraphDatabaseService db= dbFactory.newEmbeddedDatabase(DB_PATH);
			String genre_SQL="select distinct(genre) from movies_genres";
			java.sql.ResultSet genre_rs = stmt.executeQuery(genre_SQL);
			//Relationship relation;
			/*ArrayList<Genre> genreNode= new ArrayList<Genre>(); 
			ArrayList<Actors> actorNode= new ArrayList<Actors>();
			ArrayList<Directors> directorNode= new ArrayList<Directors>();
			ArrayList<Movies> movieNode= new ArrayList<Movies>();*/
			ArrayList<Node> genreAL=new ArrayList<Node>();
			ArrayList<Node> actorsAL=new ArrayList<Node>();
			ArrayList<Node> directorsAL=new ArrayList<Node>();
			ArrayList<Node> moviesAL=new ArrayList<Node>();
			
			while(genre_rs.next()){
				try ( Transaction transaction = db.beginTx() )				{	
					String genre= genre_rs.getString("genre");
					Node mynode= db.createNode(Labels_IMDB.GENRE);
					
					mynode.setProperty("name", genre);
					//Genre gn= new Genre(genre);
					//genreNode.add(gn);
					genreAL.add(mynode);
							
					//session.run("CREATE (g:GENRE {name:{name}})", parameters("name",genre));
					transaction.success();
				System.out.println("Geres created");
			}}
			String actor_SQL="select * from actors limit 500";
			java.sql.ResultSet actor_rs = stmt.executeQuery(actor_SQL);
			while(actor_rs.next()){
				try ( Transaction transaction = db.beginTx() )				{	
					int act_id= actor_rs.getInt("id");
					String firstname= actor_rs.getString("first_name");
					String lastname= actor_rs.getString("last_name");
					Node mynode= db.createNode(Labels_IMDB.ACTOR);
					mynode.setProperty("id", act_id);
					mynode.setProperty("firstName", firstname);
					mynode.setProperty("lastName", lastname);
//					Actors ac = new Actors(act_id,firstname,lastname);
//					actorNode.add(ac);
					actorsAL.add(mynode);
					transaction.success();
					
				}
			}
			String director_SQL="select id,first_name,last_name from directors limit 100";
			java.sql.ResultSet director_rs = stmt.executeQuery(director_SQL);
			while(director_rs.next()){
				try ( Transaction transaction = db.beginTx() )				{	
					int director_id= director_rs.getInt("id");
					String firstname= director_rs.getString("first_name");
					String lastname= director_rs.getString("last_name");
					Node mynode= db.createNode(Labels_IMDB.DIRECTOR);
					mynode.setProperty("id", director_id);
					mynode.setProperty("firstName", firstname);
					mynode.setProperty("lastName", lastname);
//					Directors dc = new Directors(director_id,firstname,lastname);
//					directorNode.add(dc);
					directorsAL.add(mynode);
					transaction.success();
					
				}
			}
			String movies_SQL="select id,name,year from movies limit 500";
			java.sql.ResultSet movies_rs = stmt.executeQuery(movies_SQL);
			while(movies_rs.next()){
				try ( Transaction transaction = db.beginTx() )				{	
					int year= movies_rs.getInt("year");
					int m_id= movies_rs.getInt("id");
					String name= movies_rs.getString("name");
					Node mynode= db.createNode(Labels_IMDB.MOVIE);
					mynode.setProperty("id", m_id);
					mynode.setProperty("title", name);
					mynode.setProperty("year", year);
//					Movies mv=new Movies(m_id,name,year);
//					movieNode.add(mv);
					moviesAL.add(mynode);

					transaction.success();
					
				}
			}
			System.out.println("relationship starting");
			String role_SQL=" select actor_id,movie_id,role from roles limit 50 ";
			java.sql.ResultSet roles_rs = stmt.executeQuery(role_SQL);


			while(roles_rs.next()){
				try ( Transaction transaction = db.beginTx() )				{
				int act_id=roles_rs.getInt("actor_id");
				int mv_id=roles_rs.getInt("movie_id");
				String role=roles_rs.getString("role");
//				for(Node actors: actorsAL){
//					for(Node movies: moviesAL){
//						if(String.valueOf(act_id).equals(actors.getProperty("id")) && String.valueOf(mv_id).equals(movies.getProperty("id"))){
//							Relationship relationship= (Relationship) actors.createRelationshipTo(movies, Relationship_IMBD.ACTED_IN);
//							((PropertyContainer) relationship).setProperty("role", role);
//						}
//						
//					}
//				}
				Node actorNode=db.createNode();
				Node movieNode=db.createNode();
				for (Node actors: actorsAL){
					if(String.valueOf(act_id).equals(actors.getProperty("id"))){
						 actorNode=actors;
					}
				}
				for (Node movies: moviesAL){
					if(String.valueOf(mv_id).equals(movies.getProperty("id"))){
						 movieNode=movies;
					}
				}
				Relationship relationship=  actorNode.createRelationshipTo(movieNode, Relationship_IMBD.ACTED_IN);
			relationship.setProperty("role", role);
			transaction.success();}
				}
			System.out.println("done relation");
		}
		catch(Exception e){
			System.out.println(e);
		}
		
	}

}
