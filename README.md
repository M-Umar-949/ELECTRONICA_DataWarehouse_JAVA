# ELECTRONICA Data Warehouse with JAVA

This project demonstrates a data warehousing system with OLAP queries, featuring a custom join algorithm (HYBRIDJOIN) that can be further optimized. The main goal is to efficiently manage and process data streams for insertion into a data warehouse. <br>
To run the project, execute the main file named `Controller.java`. The external libraries used include the MySQL Java connector.

## Code Files Description

### Controller.java
This is the main file. It starts its corresponding thread and also the other two threads. This file is responsible for managing the load given to the `StreamGenerator` thread. It receives the number of tuples processed in the hybrid join files and adjusts the load size, i.e., increasing or decreasing it so that the join operation does not get overloaded or underloaded.

### StreamGenerator.java
This file is responsible for generating the data streams from the `transaction.csv` file. Initially, 1000 tuples are sent to the `HybridJoin` thread. After that, the number of tuples streamed is determined based on the number of tuples processed. This file receives the processed tuples sent from the `HybridJoin` thread to `Controller.java` and then increases or decreases the load size accordingly.

### HybridJoin.java
This is the main file on which the entire project is based. It receives the streamed data from the `StreamGenerator` thread and loads the tuples into a `MultiHashMap`. Additionally, it maintains a queue that keeps a record of the joining attributes, i.e., `ProductID` in this case. Each node in the queue points to its corresponding tuple in the `MultiHashMap`. 

Then, a chunk of master data is loaded using the `BufferedReader` provided in Java. This chunk of master data is loaded based on the 10 oldest nodes. If they match with the join attribute of the master data tuple, the tuples are merged (i.e., the transaction tuple from the `MultiHashMap` and the received master data tuple from the `BufferedReader`). After merging, the tuples are added to the data warehouse. The matched tuples are then removed from the `MultiHashMap` along with their join attribute in the queue, and the variable `tuplesProcessed` is incremented, which is sent to the `Controller` thread.

Also in this thread, preprocessing is done with the date format. This preprocessing includes extracting the date with the standard format and the month, quarter, and whether it is a weekend using the `java.time` library.

The function `insertIntoDW()` is responsible for all the data insertion queries. It calls `getDateAttributes(dateReceived)` and inserts the data accordingly. Additionally, it checks for duplicate order IDs in the function and does not insert duplicates because each transaction should have a unique ID. The function `isOrderIDExists()` is responsible for this.

Moreover, many functions are implemented, such as getters of the join attribute, loader functions for the master data and received streamed data, etc.
