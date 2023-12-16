// Hybridjoin.java
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.sql.Statement;

public class Hybridjoin extends Thread 
{
    private final BlockingQueue<String> dataQueue;
    private final Map<String, String> multiHashTable;
    private final BlockingQueue<String> joinAttributeQueue;
    private int tuplesProcessed = 0;

    public Hybridjoin(BlockingQueue<String> dataQueue) 
    {
        this.dataQueue = dataQueue;
        this.multiHashTable = new HashMap<>();
        this.joinAttributeQueue = new LinkedBlockingQueue<>();
    }

    public int getTuplesProcessed() 
    {
        return tuplesProcessed;
    }

    public void resetTuplesProcessed() 
    {
        tuplesProcessed = 0;
    }
    private static final String DB_URL = "jdbc:mysql://localhost:3306/ELECTRONICA_DW";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "umar@123";
    @Override
    public void run() 
    {
        try 
        {
            while (true) 
            {
                // Read a new input chunk of sales data from the stream buffer
                String salesData = dataQueue.take();

                loadReceivedData(salesData);

                

                // Check if the master data segment needs to be loaded
                if (joinAttributeQueue.size() >= 10) 
                {
                    
                    loadMasterDataSegment();
                }

                
            }
        } 
        catch (InterruptedException e) 
        {
            e.printStackTrace();
        }
    }

    private void loadReceivedData(String data) 
    {
       
        String joinAttributeValue = extractJoinAttributeValue(data);
        multiHashTable.put(joinAttributeValue, data);
        joinAttributeQueue.offer(joinAttributeValue);  // Enqueue the join attribute
    }

    private String extractJoinAttributeValue(String data) 
    {
       
        return data.split(",")[2];
    }

 

    private String extractJoinAttributeFromMasterData(String data) 
    {
       
        return data.split(",")[0]; 
    }

    private void loadMasterDataSegment() 
    {
        try 
        {
            
            List<String> joinAttributes = new ArrayList<>();

            // Dequeuing 10 join attributes from the join attribute queue
            for (int i = 0; i < 10; i++) 
            {
                String joinAttribute = joinAttributeQueue.take();
                joinAttributes.add(joinAttribute);
            }

            // Load the corresponding segment of master data into the disk buffer
            for (String joinAttribute : joinAttributes) 
            {
                loadSegmentFromMasterData(joinAttribute);
            }

            
        } 
        catch (InterruptedException e) 
        {
            e.printStackTrace();
        }
    }


    private void loadSegmentFromMasterData(String joinAttributeValue) 
    {
        try (BufferedReader masterDataReader = new BufferedReader(new FileReader("master_data.csv"))) 
        {
            masterDataReader.readLine();
            int i =0;
            // Process each line in the master data file
            String line;
            while ((line = masterDataReader.readLine()) != null) 
            {
                String currentJoinAttributeValue = extractJoinAttributeFromMasterData(line);
                if (currentJoinAttributeValue.equals(joinAttributeValue)) 
                {
                   

                    // Check if the join attribute exists in the multi-hash map
                    if (multiHashTable.containsKey(joinAttributeValue)) 
                    {
                        String transactionTuple = multiHashTable.get(joinAttributeValue);

                       
                        String masterDataAttributes = getMasterDataAttributesFromDiskBuffer(line);

                        // If a tuple match is found, add required attributes to the transaction tuple
                        if (masterDataAttributes != null) 
                        {
                            String outputTuple = mergeTuples(transactionTuple, masterDataAttributes);
                            i++;
                            // Print the output tuple
                            System.out.println("Output Tuple: " + outputTuple);
                            
//                            insertIntoDW(outputTuple);
                            // Remove the matched tuple from the multi-hash table and join attribute queue
                            multiHashTable.remove(joinAttributeValue);
                            joinAttributeQueue.remove(joinAttributeValue);
                         
                            tuplesProcessed++;
                        }
                    }
                }
            }
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }

    private void insertIntoDW(String outputTuple) 
    {
        String[] fields = outputTuple.split(",");

        int orderID = Integer.parseInt(fields[0]);
        String date= fields[1];
        int productID = Integer.parseInt(fields[2]);
        int customerID = Integer.parseInt(fields[3]);
        String customerName = fields[4];
        String gender = fields[5];
        int quantityOrdered = Integer.parseInt(fields[6]);
        String productName = fields[7];
        double productPrice = Double.parseDouble(fields[8].replace("$", ""));
        int supplierID = Integer.parseInt(fields[9]);
        String supplierName = fields[10];
        int storeID = Integer.parseInt(fields[11]);
        String storeName = fields[12];
        if (isOrderIDExists(orderID)) {
            System.out.println("OrderID " + orderID + " already exists. Skipping insertion.");
            return;
        }

        String[] getDateATTR=getDateAttributes(date);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm");
        LocalDateTime dateTime = LocalDateTime.parse(date, formatter);
        
    	int generatedDateId,generatedCID,generatedstID,generatedsupID,generatedPID;


        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) 
        {
            // Insert into Customer table
            String insertCustomerQuery = "INSERT INTO Customer (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertCustomerQuery,Statement.RETURN_GENERATED_KEYS)) 
            {
                preparedStatement.setInt(1, customerID);
                preparedStatement.setString(2, customerName);
                preparedStatement.setString(3, gender);
                preparedStatement.executeUpdate();
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) 
                {
                    if (generatedKeys.next()) 
                    {
                        generatedCID = generatedKeys.getInt(1);
                    } 
                    else 
                    {
                        throw new SQLException("Creating customer failed, no ID obtained.");
                    }
                }
            }
            String insertDateQuery = "INSERT INTO Date_D (FullDate, DateNorm, Month, Quarter, Year, Weekend) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertDateQuery,Statement.RETURN_GENERATED_KEYS)) 
            {
            	java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(dateTime);
            	preparedStatement.setTimestamp(1, sqlTimestamp);
                preparedStatement.setDate(2, java.sql.Date.valueOf(dateTime.toLocalDate())); 
                preparedStatement.setInt(3, Integer.parseInt(getDateATTR[1]));  
                preparedStatement.setInt(4, Integer.parseInt(getDateATTR[2]));  
                preparedStatement.setInt(5, Integer.parseInt(getDateATTR[3]));  
                preparedStatement.setBoolean(6, Boolean.parseBoolean(getDateATTR[4]));  
                preparedStatement.executeUpdate();
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) 
                {
                    if (generatedKeys.next()) 
                    {
                        generatedDateId = generatedKeys.getInt(1);
                        
                    } 
                    else 
                    {
                        throw new SQLException("Creating customer failed, no ID obtained.");
                    }
                }
            }
            

            // Insert into Supplier table
            String insertSupplierQuery = "INSERT INTO Supplier (SupplierID, SupplierName) VALUES (?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSupplierQuery,Statement.RETURN_GENERATED_KEYS)) 
            {
                preparedStatement.setInt(1, supplierID);
                preparedStatement.setString(2, supplierName);
                preparedStatement.executeUpdate();
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) 
                {
                    if (generatedKeys.next()) 
                    {
                        generatedsupID = generatedKeys.getInt(1);

                    } 
                    else 
                    {
                        throw new SQLException("Creating customer failed, no ID obtained.");
                    }
                }
            }

            // Insert into Store table
            String insertStoreQuery = "INSERT INTO Store (StoreID, StoreName) VALUES (?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertStoreQuery,Statement.RETURN_GENERATED_KEYS)) 
            {
                preparedStatement.setInt(1, storeID);
                preparedStatement.setString(2, storeName);
                preparedStatement.executeUpdate();
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) 
                {
                    if (generatedKeys.next()) 
                    {
                        generatedstID = generatedKeys.getInt(1);
                        System.out.println(generatedDateId);
                    } 
                    else 
                    {
                        throw new SQLException("Creating customer failed, no ID obtained.");
                    }
                }
            }

            // Insert into Product table
            String insertProductQuery = "INSERT INTO Product (ProductID, ProductName,  SupplierID, StoreID) VALUES (?,  ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertProductQuery,Statement.RETURN_GENERATED_KEYS)) 
            {
                preparedStatement.setInt(1, productID);
                preparedStatement.setString(2, productName);
                preparedStatement.setInt(3, supplierID);
                preparedStatement.setInt(4, storeID);
                preparedStatement.executeUpdate();
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) 
                {
                    if (generatedKeys.next()) 
                    {
                        generatedPID = generatedKeys.getInt(1);
                     
                    } 
                    else 
                    {
                        throw new SQLException("Creating customer failed, no ID obtained.");
                    }
                }
            }

            // Insert into Sales table
            String insertSalesQuery = "INSERT INTO Sales (OrderID,  QuantityOrdered ,ProductPrice,DateID,ProductID,CustomerID,StoreID,SupplierID) VALUES ( ?,?,?,?,?,?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSalesQuery)) 
            {
                preparedStatement.setInt(1, orderID);
                
                preparedStatement.setInt(2, quantityOrdered);
                
                preparedStatement.setDouble(3, productPrice);
                preparedStatement.setDouble(4, generatedDateId);
                
                preparedStatement.setDouble(5, generatedPID);
                
                preparedStatement.setDouble(6, generatedCID);
                
                preparedStatement.setDouble(7, generatedstID);
                
                preparedStatement.setDouble(8, generatedsupID);
                
                preparedStatement.executeUpdate();
            }

        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }
    
    public static String[] getDateAttributes(String dateStr) 
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm");
        LocalDateTime dateTime = LocalDateTime.parse(dateStr, formatter);

        String date = Integer.toString(dateTime.getDayOfMonth());
        String month = Integer.toString(dateTime.getMonthValue());
        String quarter = Integer.toString(dateTime.get(IsoFields.QUARTER_OF_YEAR));
        String year = Integer.toString(dateTime.getYear());
        boolean isWeekend = dateTime.getDayOfWeek() == DayOfWeek.SATURDAY || dateTime.getDayOfWeek() == DayOfWeek.SUNDAY;

        return new String[]{date, month, quarter, year, Boolean.toString(isWeekend)};
    }
    
    
    private boolean isOrderIDExists(int orderID) 
    {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) 
        {
            String checkOrderIDQuery = "SELECT COUNT(*) FROM Sales WHERE OrderID = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(checkOrderIDQuery)) 
            {
                preparedStatement.setInt(1, orderID);
                try (ResultSet resultSet = preparedStatement.executeQuery()) 
                {
                    if (resultSet.next()) 
                    {
                        int count = resultSet.getInt(1);
                        return count > 0; // If count is greater than 0, OrderID exists
                    }
                }
            }
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
        return false;
    }

    private String getMasterDataAttributesFromDiskBuffer(String line) 
    {
    	 if (line != null) 
    	 {
    	        String[] attributes = line.split(",");
    	        
    	        // Ensure that the array has the expected length before accessing its elements
    	        if (attributes.length >= 7) 
    	        {
    	            return attributes[1] + "," + attributes[2] + "," + attributes[3] + "," + attributes[4] + "," +
    	                    attributes[5] + "," + attributes[6];
    	        }
    	        else 
    	        {
    	            System.err.println("Invalid format for master data line: " + line);
    	            return null;
    	        }
    	    } 
    	 else 
    	 {
    	        System.err.println("Null line encountered while processing master data");
    	        return null;
    	 }
    }

    private String mergeTuples(String transactionTuple, String masterDataAttributes) 
    {
        
        return transactionTuple + "," + masterDataAttributes;
    }

  
}
