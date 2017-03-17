
package Neo4jHW.graphNeo4j;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.index.impl.lucene.legacy.*;
import org.neo4j.graphdb.Transaction;
/*
 * @author: gayatri dudhat
 * In this program, the database is taken from MySQL and loaded in Neo4j.
 */

public class Neo4jImplement {
	static final String user="root";
	static final String password="Shubhu06081996";
	static final String url="jdbc:mysql://localhost:3306/homework1";
	static final String neo4j="bolt://localhost";
	static final File DB_PATH = new File( "C:/Users/gayat/OneDrive/Documents/Neo4j/IMDBNEW123" );
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	
	static GraphDatabaseFactory dbFactory;
	private static GraphDatabaseService db;
	private static Index<Node> nodeIndexGenre;
	private static Index<Node> nodeIndexActor;
	private static Index<Node> nodeIndexDirector;
	private static Index<Node> nodeIndexMovie;
	
	public static void main(String args[]){
		try{
			//creates connection with MySQL
			Class.forName(JDBC_DRIVER);  
			Connection con=null;
			Connection con1=null;
			Statement stmt=null;
			
			con=DriverManager.getConnection(url,user,password);
			stmt=con.createStatement();  
			System.out.println("MySQL connection done");
			
			//creates connection in Neo4j
			FileUtils.deleteRecursively( DB_PATH );
			
			dbFactory = new GraphDatabaseFactory();
			db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
			System.out.println("connection2 is done");

			registerShutdownHook();
			
			System.out.println("Node genration");
			//Genre node creation
			String genre_SQL="select distinct(genre) from movies_genres";
			java.sql.ResultSet genre_rs = stmt.executeQuery(genre_SQL);
			int i=0;
			
				
				  Transaction transactionGenre = db.beginTx() ;
					while(genre_rs.next()){
					nodeIndexGenre= db.index().forNodes("GENRE");

					String genre= genre_rs.getString("genre");
					Node node = db.createNode(Labels_IMDB.GENRE);
			        node.setProperty( "genre", genre );
			        nodeIndexGenre.add( node, "genre", genre );
			        transactionGenre.success();
					
				
			}
					
					transactionGenre.close();
					

			System.out.println("Genres created");
			
			//Actor node creation

			String actor_SQL="select id,first_name,last_name from actors";
			java.sql.ResultSet actor_rs = stmt.executeQuery(actor_SQL);
			
				 Transaction transactionAct = db.beginTx();
					int acti=0;
					nodeIndexActor= db.index().forNodes("ACTOR");

					while(actor_rs.next()){
					acti++;
					int act_id= actor_rs.getInt("id");
					String firstname= actor_rs.getString("first_name");
					String lastname= actor_rs.getString("last_name");
					Node mynode= db.createNode(Labels_IMDB.ACTOR);
					mynode.setProperty("id", act_id);
					mynode.setProperty("firstName", firstname);
					mynode.setProperty("lastName", lastname);
			        nodeIndexActor.add( mynode, "id", act_id );
					
			        if(acti==10000){
			        	transactionAct.success();
			        	transactionAct.close();
			        	transactionAct=db.beginTx();
						acti=0;
			        	
			        }}
					
					transactionAct.success();
					transactionAct.close();
					
					
				
			
			System.out.println("Actors created");
			//Director node creation

			String director_SQL="select id,first_name,last_name from directors";
			java.sql.ResultSet director_rs = stmt.executeQuery(director_SQL);
			int directori=0;
				 Transaction transactionDirector = db.beginTx();
					while(director_rs.next()){
						directori++;
					nodeIndexDirector= db.index().forNodes("DIRECTOR");


					int director_id= director_rs.getInt("id");
					String firstname= director_rs.getString("first_name");
					String lastname= director_rs.getString("last_name");
					Node node= db.createNode(Labels_IMDB.DIRECTOR);	
					node.setProperty("id", director_id);
					node.setProperty("firstName", firstname);
					node.setProperty("lastName", lastname);
			        nodeIndexDirector.add( node, "id", director_id );
					

			        if(directori==10000){
			        	transactionDirector.success();
			        	transactionDirector.close();
			        	transactionDirector=db.beginTx();
						directori=0;
					
				}}
			        transactionDirector.success();
					
			
					transactionDirector.close();
			System.out.println("Directors created");
			//Movie node creation

			String movies_SQL="select id,name,year from movies ";
			java.sql.ResultSet movies_rs = stmt.executeQuery(movies_SQL);
			
				 Transaction transactionMovie = db.beginTx() ;
					int moviei=0;
					while(movies_rs.next()){
						moviei++;
					 nodeIndexMovie= db.index().forNodes("MOVIE");

					int year= movies_rs.getInt("year");
					int m_id= movies_rs.getInt("id");
					String name= movies_rs.getString("name");
					Node node = db.createNode(Labels_IMDB.MOVIE);
			        node.setProperty("id", m_id);
					node.setProperty("title", name);
					node.setProperty("year", year);
			        nodeIndexMovie.add( node, "id", m_id );


			        if(moviei==10000){
			        	transactionMovie.success();
			        	transactionMovie.close();
			        	transactionMovie=db.beginTx();
						moviei=0;
					
				}}
			        transactionMovie.success();
				
					transactionMovie.close();
			
			System.out.println("Movies created");
			//Role relationship creation

			String role_SQL=" select actor_id,movie_id,role from roles";
			java.sql.ResultSet roles_rs = stmt.executeQuery(role_SQL);

			 Transaction transactionRoles = db.beginTx();
				int rolei=0;
				while(roles_rs.next()){
					rolei++;
					int act_id=roles_rs.getInt("actor_id");
					int mv_id=roles_rs.getInt("movie_id");
					String role=roles_rs.getString("role");
					Node foundAct = nodeIndexActor.get( "id",act_id ).getSingle();
					Node foundMov = nodeIndexMovie.get( "id",mv_id ).getSingle();
					if(foundAct != null && foundMov!= null){
						Relationship relationship= foundAct.createRelationshipTo(foundMov, Relationship_IMBD.ACTED_IN);
						relationship.setProperty("role", role);
					}
					if(rolei==10000){
						transactionRoles.success();
						transactionRoles.close();
						transactionRoles=db.beginTx();
						rolei=0;
					
				}}
					
					transactionRoles.success();
				
				transactionRoles.close();
			
			
			System.out.println("role relation");
			//Movie Genre relationship creation

			String movieGenreSQL=" select * from movies_genres";
			java.sql.ResultSet mg_rs = stmt.executeQuery(movieGenreSQL);
			 Transaction transactionMG = db.beginTx() ;
				int mgi=0;
				while(mg_rs.next()){
					mgi++;
					int movie_id=mg_rs.getInt("movie_id");
					String genre=mg_rs.getString("genre");
					Node foundMov = nodeIndexMovie.get( "id",movie_id ).getSingle();
					Node foundGen = nodeIndexGenre.get( "genre",genre ).getSingle();
					if(foundMov != null && foundGen!= null){
						Relationship relationship= foundMov.createRelationshipTo(foundGen, Relationship_IMBD.MOVIE_GENRE);
					}
					if(mgi==10000){
						transactionMG.success();
						transactionMG.close();
						transactionMG=db.beginTx();
						mgi=0;
					
				}}
					transactionMG.success();
				
				transactionMG.close();
			
			System.out.println("movie genre relationship");
			//Director Genre relationship creation

			String directorGenreSQL=" select director_id, genre from directors_genres";
			java.sql.ResultSet dg_rs = stmt.executeQuery(directorGenreSQL);
			Transaction transactionDG = db.beginTx();
				int dgi=0;
				while(dg_rs.next()){
					dgi++;
					int director_id=dg_rs.getInt("director_id");
					String genre=dg_rs.getString("genre");
					Node foundDir = nodeIndexDirector.get( "id",director_id ).getSingle();
					Node foundGen = nodeIndexGenre.get( "genre",genre ).getSingle();
					if(foundDir != null && foundGen!= null){
						Relationship relationship= foundDir.createRelationshipTo(foundGen, Relationship_IMBD.DIRECTOR_GENRE);
					}
					if(dgi==10000){
						transactionDG.success();
						transactionDG.close();
						transactionDG=db.beginTx();
						dgi=0;
					
				}}
					transactionDG.success();
					transactionDG.close();
			System.out.println("director genre relationship");
			//Movie director relationship creation

			String m_SQL="select * from movies_directors";
			java.sql.ResultSet md_rs = stmt.executeQuery(m_SQL);

			Transaction transactionMD = db.beginTx();
				int mdi=0;
				while(md_rs.next()){
					mdi++;
					int director_id=md_rs.getInt("director_id");
					int mv_id=md_rs.getInt("movie_id");

					Node foundDir = nodeIndexDirector.get( "id",director_id ).getSingle();
					Node foundMov = nodeIndexMovie.get( "id",mv_id ).getSingle();
					if(foundDir != null && foundMov!= null){
						Relationship relationship= foundDir.createRelationshipTo(foundMov, Relationship_IMBD.DIRECTED_BY);
					}
					if(mdi==10000){
						transactionMD.success();
						transactionMD.close();
						transactionMD=db.beginTx();
						mdi=0;
					
				}}
					transactionMD.success();
					
					
				
				transactionMD.close();
			 System.out.println("director movie relationship");
			
			

			
			
		}
		catch(Exception e){
			System.out.println(e);
		}
		
	}
	/**
	 * This function is used to shutdown the Neo4j connection
	 */

	private static void shutdown()
    {
        db.shutdown();
    }
	private static void registerShutdownHook()
    {
        
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );

}
}
