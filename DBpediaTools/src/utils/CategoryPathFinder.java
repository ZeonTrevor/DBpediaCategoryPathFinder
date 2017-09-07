package utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class CategoryPathFinder {
	
	private static final Logger log = Logger.getLogger(CategoryPathFinder.class);
	
	private static String PREFIXES = "PREFIX  dbr: <http://dbpedia.org/resource/>\n" +
            "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX  onto: <http://dbpedia.org/ontology/>\n" +
            "PREFIX  dct: <http://purl.org/dc/terms/>\n" +
            "PREFIX  dbc: <http://dbpedia.org/resource/Category:>\n" +
            "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n";

	private static String sparqlEndpoint = "http://live.dbpedia.org/sparql";
	
	//Extract categories of entity using `dct:subject` field
	public static HashSet<String> getEntityCategories(String entity)
	{
		HashSet<String> categories = new HashSet<String>();
		String entityQuery = PREFIXES +
                "SELECT ?o WHERE\n" +
                "  { dbr:<entity> dct:subject ?o .\n" +
                "  }\n" +
                "";
		entityQuery = entityQuery.replace("<entity>", entity);
		
		QueryExecution qExe = QueryExecutionFactory.sparqlService(sparqlEndpoint, QueryFactory.create(entityQuery));
		ResultSet results = qExe.execSelect();
		//ResultSetFormatter.out(System.out, results, query) ;
		while(results.hasNext())
		{
			QuerySolution qSol = results.nextSolution();
			String categoryUrl = qSol.get("o").toString();        	
			categories.add(categoryUrl);
		}
		
		return categories;
	}
	
	public static void main(String[] args) throws InterruptedException
    {
	
		long startTime = System.currentTimeMillis();
		Queue<String> categoryQueue = new LinkedList<String>();
		HashSet<String> categorySeen = new HashSet<String>();
		HashSet<String> queryCategories = new HashSet<String>();
		boolean traverseHierarchy = true;

		String s1 = PREFIXES +
				"SELECT ?o WHERE\n" +
				" { <category_url> skos:broader ?o .\n" +
				" }\n";
		
		queryCategories.addAll(getEntityCategories("Information_retrieval"));
		
        log.info("Query Categories are: " + queryCategories);
        
        HashSet<String> entityCategories = getEntityCategories("CS_gas");

        for(String category: entityCategories)
        {
        	if(queryCategories.contains(category))
        	{	traverseHierarchy  = false;
        		break;
        	}	
        	categoryQueue.add(category);
        }

        if(traverseHierarchy)
        {
        	boolean stopTraversing = true;
        	int loopCount = 0;
	        while(categoryQueue.size() > 0 && stopTraversing)
	        {
	        	String categoryUrl = categoryQueue.poll();
	        	categorySeen.add(categoryUrl);
	        	String q1 = s1.replace("category_url", categoryUrl);
	        	
	        	//System.out.println("Child:" + categoryUrl);
	
	        	QueryExecution qExe3 = QueryExecutionFactory.sparqlService("http://live.dbpedia.org/sparql", QueryFactory.create(q1) );
	        	ResultSet rs2 = qExe3.execSelect();
	        	while(rs2.hasNext())
	        	{
	        		QuerySolution qSol2 = rs2.nextSolution();
	        		String parent_category_url = qSol2.get("o").toString();
	        		
	        		if(queryCategories.contains(parent_category_url))
	        		{
	        			log.info("Found match to query categories: " + parent_category_url);
	        			stopTraversing = false;
	        			break;
	        		}
	        		
	        		if(!categorySeen.contains(parent_category_url))
	        		{
	        			categoryQueue.add(parent_category_url);
	        			categorySeen.add(parent_category_url);
	        		}
	        		
	        		//System.out.println(parent_category_url);
	        	}
	        	
	        	//To avoid sending too many requests per second to the endpoint
	        	//Else the endpoint will throw HTTP exception:503 Service unavailable
	        	Thread.sleep(500);
	        	//System.out.println();
	        	loopCount++;
	        }
	        
	        log.info("Total no. of loops: " + loopCount);
        }

        log.info("Time taken: " + (System.currentTimeMillis() - startTime)/1000);
        
    }
}
