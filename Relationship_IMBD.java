package Neo4jHW.graphNeo4j;

import org.neo4j.graphdb.RelationshipType;

public enum Relationship_IMBD implements RelationshipType {
	ACTED_IN, MOVIE_GENRE,DIRECTED_BY, DIRECTOR_GENRE;
}
