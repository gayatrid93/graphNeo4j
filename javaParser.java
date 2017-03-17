package Neo4jHW.graphNeo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
/**
 * In this program, a java parser is implemented which converts the
 * given format to the Cypher format. The input is the text file.
 * @author gayat
 *
 */
public class javaParser  {
	public static void main(String args[]) throws FileNotFoundException{
	
	try {
		String filePath="C:/Users/gayat/workspace/Graph_database/src/Neo4jHW/graphNeo4j/query52";

		BufferedReader br= new BufferedReader(new FileReader(filePath));
		String line;
		
		HashMap<String, ArrayList<String>> relation = new HashMap<String, ArrayList<String>>();
		ArrayList<ArrayList<String>> related= new ArrayList<ArrayList<String>>();
		StringBuilder match= new StringBuilder("MATCH ");
		StringBuilder where=new StringBuilder("WHERE ");
		StringBuilder returned=new StringBuilder("RETURN ");
		//Reading the file
		while ((line=br.readLine())!= null){
			String words[]= line.split("\t");
			if(words.length>2 ||line.length()>5){
				ArrayList<String> parameters= new ArrayList<String>();
				for (int i=1;i<words.length;i++){
					parameters.add(words[i]);					
				}
				relation.put(words[0], parameters);
			}else{
				ArrayList<String> parameters= new ArrayList<String>();
				parameters.add(words[0]);
				parameters.add(words[1]);
				related.add(parameters);
			}
			
		}
		Set<String> alreadyContains= new HashSet<String>();
		//Manipulating to get the Cypher format
		for(ArrayList<String> str: related){
			for(int i=0;i<str.size();i++){
				ArrayList<String> found=relation.get(str.get(i));
				String u1= str.get(i)+":"+found.get(0);
				if(alreadyContains.contains(u1)){
					match.append("("+str.get(i)+")");
				}
				else{
					alreadyContains.add(u1);
					match.append("("+u1+")");
				}
				
					for(int j=1;j<found.size();j++){
						String where1=str.get(i)+":"+found.get(j);
						if(alreadyContains.contains(where1)==false){
							alreadyContains.add(where1);
							where.append(where1);
							where.append(" AND ");

						}
						
					}
					if(i==0)
						match.append("--");
				
			}match.append(",\n");
			
		}
		Iterator it= relation.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pair= (Map.Entry)it.next();
			returned.append(pair.getKey()+",");
			
		}
		//printing out in Cypher format
		System.out.println((match.substring(0, match.length()-2)).toString());
		System.out.println((where.substring(0, where.length()-4)).toString());
		System.out.println((returned.substring(0, returned.length()-1)).toString());
		
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	}

}
