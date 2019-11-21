//Kamonwan Tangamornphiboon 6088034 Section: 2
//Patakorn Jearat 6088065 Section: 2
//Matchatta Toyaem 6088169 Section: 2

import javax.imageio.IIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		int termId, docFreq, BufferSize=4;
		List<Integer> docList = new ArrayList<>();
		//Read termId and put it in to Buffer
		ByteBuffer Buffer = ByteBuffer.allocate(BufferSize);
		try{
			int read = fc.read(Buffer);
			if(read==-1){
				return null;
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		//Get data from Buffer and reset it
		Buffer.flip();
		termId = Buffer.getInt();

		//Read doc frequency and put it in to buffer
		Buffer = ByteBuffer.allocate(BufferSize);
		try{
			fc.read(Buffer);
		}catch (Exception e){
			e.printStackTrace();
		}
		Buffer.flip();
		//Get data from Buffer and reset it
		docFreq = Buffer.getInt();

		//Read all docId
		for(int i=0; i<docFreq; i++){
			Buffer = ByteBuffer.allocate(BufferSize);
			try{
				//Get docId from file and put it into Buffer
				fc.read(Buffer);
			}catch (Exception e){
				e.printStackTrace();
			}
			//Get data from Buffer and reset it
			Buffer.flip();
			docList.add(Buffer.getInt());
		}
		return new PostingList(termId, docList);
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		int BufferSize = 8;
		//Allocate buffer for termId and doc frequency
		ByteBuffer Buffer = ByteBuffer.allocate(BufferSize);
		//Put termId to the Buffer
		Buffer.putInt(p.getTermId());
		//Put doc frequency to the Buffer
		Buffer.putInt(p.getList().size());
		//Get data from buffer and reset its
		Buffer.flip();
		//Write data to file(fc)
		try{
			fc.write(Buffer);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		//Write all docId to file(fc)
		for(int i : p.getList()){
			ByteBuffer listBuffer = ByteBuffer.allocate(BufferSize/2);
			listBuffer.putInt(i);
			listBuffer.flip();
			try{
				fc.write(listBuffer);
			}catch (Exception e){
				e.printStackTrace();
			}
		}

	}
}

