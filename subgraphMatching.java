package Neo4jHW.graphNeo4j;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Transaction;
/**
 * This program implements the naive subgraph matching algorithm to find
 * out the subgraph using Neo4j connection
 * @author gayatri dudhat
 *
 */
public class subgraphMatching {
	static final File DB_PATH = new File( "C:/Users/gayat/OneDrive/Documents/Neo4j/IMDBNEW123" );
	static GraphDatabaseFactory dbFactory;
	private static GraphDatabaseService db;

	public static void main(String args[]){
		subgraphMatching sub= new subgraphMatching();
		//connection with Neo4j
		dbFactory = new GraphDatabaseFactory();
		db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		System.out.println("connection2 is done");

		registerShutdownHook();
		
		
//		Transaction begingenre=db.beginTx();
//		long startTime=System.currentTimeMillis();
//		//HashMap to store the path
//
//		Map<String, ArrayList<String>> actorcount1=new HashMap<String , ArrayList<String>>();
//		// finding all the nodes for search space
//		ResourceIterator<Node> actor= db.findNodes(Labels_IMDB.ACTOR);
//
//		while(actor.hasNext()){
//			Node act = actor.next();
//
//			//implementing the matching and backtracking search
//			for(Relationship relation: act.getRelationships(Relationship_IMBD.ACTED_IN)){
//				Node movie= relation.getOtherNode(act);
//				if(sub.genreIsComedy(movie)==true){
//					if(sub.directorIsWoody(movie)==true){
////						storing the path if it satisfy the condition
//						String key=act.getProperty("firstName").toString()+" "+act.getProperty("lastName").toString();
//						if(!actorcount1.containsKey(key)){
//							ArrayList<String> movieList=new ArrayList<String>();
//							movieList.add(movie.getProperty("title").toString());
//							actorcount1.put(key, movieList);
//						}
//						else{
//							ArrayList<String> movieList= actorcount1.get(key);
//							movieList.add(movie.getProperty("title").toString());
//							actorcount1.put(key, movieList);
//						}
//
//						
//					}
//				}
//		}
//					
//		
//	}
//		begingenre.success();
//		begingenre.close();
//		long endTime=System.currentTimeMillis();
//		
//		int count1=0;
//		//displaying the path
//		for(Map.Entry<String, ArrayList<String>> entry: actorcount1.entrySet()){
//			if(entry.getValue().size() >3){
//				System.out.println(entry.getKey()+":"+entry.getValue());
//				System.out.println(" ");
//				count1++;
//			}
//		}
//		System.out.println("Total actor count: "+count1);
//		System.out.println("Total time required to execute the query is: "+(endTime-startTime));
		
		Transaction beginMovie= db.beginTx();
		ResourceIterator<Node> movies= db.findNodes(Labels_IMDB.MOVIE);
		while(movies.hasNext()){
			Node mov=movies.next();
			if(sub.genreIsDrama(mov)==true){
				ArrayList name;
				if((name=sub.isDirectorActor(mov)) != null){
					
					
				}
				
				
			}
		}
		beginMovie.success();
		beginMovie.close();
	
	}
	private ArrayList isDirectorActor(Node mov) {
		// TODO Auto-generated method stub
		HashMap directors= new HashMap<String,Integer>();
		for(Relationship relation: mov.getRelationships(Relationship_IMBD.DIRECTED_BY)){
			Node director=relation.getOtherNode(mov);
			String key=director.getProperty("firstName").toString()+" "+director.getProperty("lastName").toString();
			directors.put(key, 0);
			
		}
		for(Relationship relation: mov.getRelationships(Relationship_IMBD.ACTED_IN)){
			Node actor=relation.getOtherNode(mov);
			ArrayList<String> arr=new ArrayList<String>();
			String key=actor.getProperty("firstName").toString()+" "+actor.getProperty("lastName").toString();
			if(directors.containsKey(key)==true){
				arr.add(actor.getProperty("firstName").toString());
				arr.add(actor.getProperty("lastName").toString());
				return arr;
				
			}
				 
		}
		return null;
	}
	private boolean genreIsDrama(Node mov) {
		// TODO Auto-generated method stub
		for(Relationship relation: mov.getRelationships(
		         Direction.OUTGOING, Relationship_IMBD.MOVIE_GENRE)){
					Node genre1 = relation.getOtherNode(mov);
					String genre= (String) genre1.getProperty("genre");
					if(genre.equals("Comedy")){
						return true;
						
					}
		}
		return false;
	}
	/**
	 * This function checks whether the director is Woody Allen or not
	 * @param movie Node movie
	 * @return Boolean true or false
	 */
	private boolean directorIsWoody(Node movie) {
		for(Relationship relation: movie.getRelationships(
		         Direction.INCOMING, Relationship_IMBD.DIRECTED_BY)){ 
			Node director= relation.getOtherNode(movie);
			String firstname= (String) director.getProperty("firstName");
			String lastname= (String) director.getProperty("lastName");
			if(firstname.equals("Woody")&& lastname.equals("Allen")){
					return true;
			}
		}
		return false;
	}
	/**
	 * This function checks whether the movie has genre comedy or not
	 * @param movie Node movie
	 * @return Boolean true or false
	 *
	 */
	private  boolean genreIsComedy(Node movie) {
		for(Relationship relation: movie.getRelationships(
         Direction.OUTGOING, Relationship_IMBD.MOVIE_GENRE)){
			Node genre1 = relation.getOtherNode(movie);
			String genre= (String) genre1.getProperty("genre");
			if(genre.equals("Comedy")){
				return true;
				
			}		
			
			}
		return false;
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
