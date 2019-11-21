//Kamonwan Tangamornphiboon 6088034 Section: 2
//Patakorn Jearat 6088065 Section: 2
//Matchatta Toyaem 6088169 Section: 2

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
			throws IOException {
		/*
		 * TODO: Your code here
		 *	 
		 */
		//Call writingPosting from BasicIndex
		index.writePosting(fc, posting);
	}
	

	 /**
     * Pop next element if there is one, otherwise return null
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }
	
    
   
	
	/**
	 * Main method to start the indexing process.
	 * @param method		:Indexing method. "Basic" by default, but extra credit will be given for those
	 * 			who can implement variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname	:relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname	:relative path to the output directory to store index. You must not assume
	 * 			that this directory exist. If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException 
	{
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}
		
		   
		/* Get output directory*/
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}
		
		/*	TODO 1: delete all the files/sub folder under outdir
		 * 
		 */
		//Delete subfolder and files in destination directory.
		Files.walkFileTree(outdir.toPath(), new SimpleFileVisitor<Path>(){
			//Search file for delete
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			//Delete subdirectory
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if(exc != null){
					throw exc;
				}
				else{
					if(dir != outdir.toPath()) {//Delete everything except destination directory
						Files.delete(dir);
					}
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			//Throw error when file directory/file goes wrong
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				throw exc;
			}
		});

		
		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}
		
		

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();
		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());

			//System.out.println("Processing block "+block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();
			TreeMap<Integer, HashSet<Integer>> postingMap = new TreeMap<>();//Use to mapping term ID and document ID list
			/* For each file */
			for (File file : filelist) {
				//System.out.println("Processing block "+file.getName());
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				
				 // use pre-increment to ensure docID > 0
                int docId = ++docIdCounter;
                docDict.put(fileName, docId);
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * TODO 2: Your code here
						 *       For each term, build up a list of
						 *       documents in which the term occurs
						 */
						if(!termDict.containsKey(token)){// if term is a new term
							int termId = ++wordIdCounter;
							termDict.put(token, termId);
							HashSet<Integer> docList = new HashSet<>();//Use hash set to remove duplicate element in list
							docList.add(docId);
							postingMap.put(termId, docList);
						}
						else{ //if term have already in termDict
							if(!postingMap.containsKey(termDict.get(token))){//if postingMap does not contain that termId
								HashSet<Integer> docList = new HashSet<>();
								docList.add(docId);
								postingMap.put(termDict.get(token), docList);
							}
							else{
								postingMap.get(termDict.get(token)).add(docId);
							}
						}
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}
			
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			/*
			 * TODO 3: Your code here
			 *       Write all posting lists for all terms to file (bfc) 
			 */
			//Write posting list to file(bfc) by calling writePosting method.
			for(int termId : postingMap.keySet()){
				PostingList posting = new PostingList(termId, new ArrayList<>(postingMap.get(termId)));
				writePosting(bfc.getChannel(), posting);
			}
			bfc.getChannel().close();
			bfc.close();
		}

		/* Required: output total number of files. */
		//System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");


			/*
			 * TODO 4: Your code here
			 *       Combine blocks bf1 and bf2 into our combined file, mf
			 *       You will want to consider in what order to merge
			 *       the two blocks (based on term ID, perhaps?).
			 *       
			 */
			//Create list to store posting lists before merging them
			List<PostingList> postingList1 = new ArrayList<>();
			List<PostingList> postingList2 = new ArrayList<>();
			//Create list to store merged posting list
			List<PostingList> combinePostingList = new ArrayList<>();

			//Get all posting list from file(bf1 and bf2)
			PostingList postingList;
			while ((postingList = index.readPosting(bf1.getChannel()))!=null){
				postingList1.add(postingList);
			}
			while ((postingList = index.readPosting(bf2.getChannel()))!=null){
				postingList2.add(postingList);
			}

			//Merge posting list by using BSBI concept
			while(postingList1.size()>0 && postingList2.size()>0){
				int termId1 = postingList1.get(0).getTermId(), termId2 = postingList2.get(0).getTermId();
				if(termId1 < termId2){
					combinePostingList.add(postingList1.get(0));
					postingList1.remove(0);
				}
				else if(termId1 > termId2){
					combinePostingList.add(postingList2.get(0));
					postingList2.remove(0);
				}
				else{
					HashSet<Integer> docTemp = new HashSet<>();
					docTemp.addAll(postingList1.remove(0).getList());
					docTemp.addAll(postingList2.remove(0).getList());
					List<Integer> docIdList = new ArrayList<>(docTemp);
					docIdList.sort(Comparator.naturalOrder());
					combinePostingList.add(new PostingList(termId1, docIdList));
				}
			}

			//If there are remained posting list add all of them to combinePostingList
			while(!postingList1.isEmpty() || !postingList2.isEmpty()){
				if(postingList1.isEmpty()){
					combinePostingList.add(postingList2.remove(0));
				}
				else if(postingList2.isEmpty()){
					combinePostingList.add(postingList1.remove(0));
				}
			}
			//Write merged posting list to file(mf)
			int byteCounter=0;
			for(PostingList tempPostingList : combinePostingList){
				Pair<Long, Integer> pair = new Pair<>(Long.valueOf(byteCounter), tempPostingList.getList().size());
				//Put posting list, position of index file, and document frequency to postingDict
				postingDict.put(tempPostingList.getTermId(), pair);
				writePosting(mf.getChannel(), tempPostingList);
				//Get position of termID, doc frequency, and posting list in each posting list
				byteCounter += 8;
				byteCounter+= tempPostingList.getList().size()*4;
			}
			
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
		
		return totalFileCount;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];
		

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
