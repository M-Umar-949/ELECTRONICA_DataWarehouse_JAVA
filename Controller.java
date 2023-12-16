// Controller.java
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Controller 
{
    public static void main(String[] args) 
    {
        BlockingQueue<String> dataQueue = new LinkedBlockingQueue<>();

        // Creating and start StreamGenerator thread
        StreamGenerator streamGenerator = new StreamGenerator(dataQueue);
        streamGenerator.start();

        // Creating and starting HybridJoin thread
        Hybridjoin hybridJoin = new Hybridjoin(dataQueue);
        hybridJoin.start();

        // Creating and starting Controller thread
        ControllerThread controllerThread = new ControllerThread(streamGenerator, hybridJoin);
        controllerThread.start();
    
    }

    private static class ControllerThread extends Thread 
    {
        private final StreamGenerator streamGenerator;
        private final Hybridjoin hybridJoin;
        private final int targetLoadSize = 1000;

        public ControllerThread(StreamGenerator streamGenerator, Hybridjoin hybridJoin) 
        {
            this.streamGenerator = streamGenerator;
            this.hybridJoin = hybridJoin;
        }

        @Override
        public void run() 
        {
            try 
            {
                while (true) 
                {
                    Thread.sleep(100); 
                    
                    // Getting tuples processed and current load size
                    int currentLoadSize = streamGenerator.getCurrentLoadSize();
                    int tuplesProcessed = hybridJoin.getTuplesProcessed();

                    // Logic for the load size
                    if (currentLoadSize < targetLoadSize && tuplesProcessed > targetLoadSize) 
                    {
                        streamGenerator.increaseLoadSize();
                    } 
                    else if (currentLoadSize > targetLoadSize && tuplesProcessed < targetLoadSize) 
                    {
                        streamGenerator.decreaseLoadSize();
                    }

                    // Reset the tuplesProcessed count for the next iteration
                    hybridJoin.resetTuplesProcessed();
                }
            } 
            catch (InterruptedException e) 
            {
                e.printStackTrace();
            }
        }
    }
}
