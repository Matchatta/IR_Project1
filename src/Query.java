//Kamonwan Tangamornphiboon 6088034 Section: 2
//Patakorn Jearat 6088065 Section: 2
//Matchatta Toyaem 6088169 Section: 2

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

public class Query {

	// Term id -> position in index file
	private  Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private  Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private  Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private  Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private  BaseIndex index = null;
	

	//indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;
	
	/* 
	 * Read a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private  PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		/*
		 * TODO: Your code here
		 */
		//Read posting list from file(fc) at specific termId
		return index.readPosting(fc);
	}
	
	
	public void runQueryService(String indexMode, String indexDirname) throws IOException
	{
		//Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode+"Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		//Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}
		
		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname,
				"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();
		
		this.running = true;
	}
    
	public List<Integer> retrieve(String query) throws IOException
	{	if(!running)
		{
			System.err.println("Error: Query service must be initiated");
		}


		/*
		 * TODO: Your code here
		 *       Perform query processing with the inverted index.
		 *       return the list of IDs of the documents that match the query
		 *
		 */
		//Return null if query is null
		if(query==null){
			return null;
		}
		//Find posting list by using query
		else{
			Stack<List<Integer>> postingStack = new Stack<>(); //use stack to collect posting list
			HashSet<String> tokens = new HashSet();//Use hash set to collect token and delete duplicate token
			tokens.addAll(Arrays.asList(query.split("\\s+")));
			for(String token : tokens){
				if(!termDict.containsKey(token)){//Return null if token is not contains in termDict
					return null;
				}
				else{//Get posting list at specific term by using token
					indexFile.seek(posDict.get(termDict.get(token)));
					//Call readPosting method
					PostingList postingList = readPosting(indexFile.getChannel(), termDict.get(token));
					//Push posting list to the stack
					postingStack.push(postingList.getList());
				}
			}
			if(postingStack.isEmpty()){//Return null if stack does not have any element
				return null;
			}
			else{
				if(postingStack.size() == 1){//If there is one query, pop stack and return posting list
					HashSet hashSet = new HashSet();
					hashSet.addAll(postingStack.pop());
					List Answer = new ArrayList(hashSet);
					return Answer;
				}
				else {//If there are more than one query, use intersection to get conjunctive list. Do it until there is one element
					while(postingStack.size()>1){
						List<Integer> answer = new ArrayList<>();
						List<Integer> postingListA = postingStack.pop();
						List<Integer> postingListB = postingStack.pop();
						for(Integer docId : postingListA){
							if (postingListB.contains(docId)){
								if(!answer.contains(docId)) {
									answer.add(docId);
								}
							}
						}
						postingStack.push(answer);
					}
					HashSet hashSet = new HashSet();
					hashSet.addAll(postingStack.pop());
					List Answer = new ArrayList(hashSet);
					if(Answer.isEmpty()){//If there are no element in Answer return null
						return null;
					}
					else{//Return Answer
						return Answer;
					}
				}
			}
		}

	}
	
    String outputQueryResult(List<Integer> res) {
        /*
         * TODO: 
         * 
         * Take the list of documents ID and prepare the search results, sorted by lexicon order. 
         * 
         * E.g.
         * 	0/fine.txt
		 *	0/hello.txt
		 *	1/bye.txt
		 *	2/fine.txt
		 *	2/hello.txt
		 *
		 * If there no matched document, output:
		 * 
		 * no results found
		 * 
         * */
        StringBuilder outPut = new StringBuilder();
        List<String> Result = new ArrayList<>();
        // If res(Result) is null append no result found in outPut(StringBuilder)
    	if(res == null){
			outPut.append("no result found");
		}
    	//Append document name in outPut(StringBuilder)
    	else{
    		for(int docId : res){
				Result.add(docDict.get(docId));
			}
    		//Get document name from docDict by using docId and sort the result
    		Result.sort(String::compareToIgnoreCase);
    		for(String FileName: Result){
    			outPut.append(FileName+"\n");
			}
		}
    	return outPut.toString();
    }
	
	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];
		
		Query queryService = new Query();
		queryService.runQueryService(className, input);
		
		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}
		
		br.close();
	}
	
	protected void finalize()
	{
		try {
			if(indexFile != null)indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

