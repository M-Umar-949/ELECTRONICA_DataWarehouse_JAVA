// StreamGenerator.java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StreamGenerator extends Thread 
{
    private final BlockingQueue<String> dataQueue;
    
    private final String transactionsCsvFile = "transactions.csv"; 
    private int loadSize = 1000;

    public StreamGenerator(BlockingQueue<String> dataQueue) 
    {
        this.dataQueue = dataQueue;
    }

    public int getCurrentLoadSize() 
    {
        return loadSize;
    }

    public void increaseLoadSize() 
    {
        loadSize += 1000; // Increase load size by 1000 (adjust as needed)
    }

    public void decreaseLoadSize() 
    {
        loadSize -= 100; // Decrease load size by 100 (adjust as needed)
        if (loadSize < 100) 
        {
            loadSize = 1000; // Ensure load size is at least 1000
        }
    }

    @Override
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(transactionsCsvFile));

          
            reader.readLine();

            // Loadimg the initial tuples into the queue for the first iteration
            loadInitialTuples(reader);

            while (!Thread.interrupted()) 
            {
                // Loading a new input chunk for each subsequent iteration
                loadNewInputChunk(reader);

                // Sleep for a short duration before generating the next data
                Thread.sleep(100); 
            }

            reader.close();
        } 
        catch (IOException | InterruptedException e) 
        {
            e.printStackTrace();
        }
    }

    private void loadInitialTuples(BufferedReader reader) throws IOException, InterruptedException 
    {
        for (int i = 0; i < loadSize; i++) 
        {
            String line = reader.readLine();
            if (line != null) 
            {
                dataQueue.put(line);
            } 
            else 
            {
                break; 
            }
        }
    }

    private void loadNewInputChunk(BufferedReader reader) throws IOException, InterruptedException 
    {
        String line = reader.readLine();
        if (line != null) 
        {
            dataQueue.put(line);
        } 
        else 
        {
            
        	reader.close();
            reader = new BufferedReader(new FileReader(transactionsCsvFile));
            reader.readLine(); 

            // Loading the initial tuples into the queue for the next iteration
            loadInitialTuples(reader);
        }
    }

    public static void main(String[] args) 
    {
        // Create a blocking queue to pass data between StreamGenerator and other components
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(1000);

        // Creating an instance of StreamGenerator
        StreamGenerator streamGenerator = new StreamGenerator(dataQueue);

        // Start the StreamGenerator thread
        streamGenerator.start();

        
        streamGenerator.interrupt();
    }
}
