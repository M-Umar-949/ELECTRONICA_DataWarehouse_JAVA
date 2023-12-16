# ELECTRONICA_DataWarehouse_JAVA
Data Warehousing demonstration with OLAP queries. Custom join (HYBRIDJOIN) is implemented.

To run the Project run the main file named "Controller.java"
The external libraries used are the mySQL- java connector 


# Code Files Description
## Controller.java:
	This is the main file. This file starts its corresponding thread and also the other two threads. This file is responsible for the load given to the stream generator thread. It receives the number of tuples proceed in the hybrid join files and adjust the load size i.e., increasing or decreasing it so that the join operation does not get overload or underload.

## StreamGenerator.java:
	This file is responsible for generating the data streams from the transaction.csv file. Initially 1000 tuples are sent to the Hybrid join thread and the after that the number of tuples streamed are being decided from the number of tuples processed. This file receives the tuples processed which were sent from Hybrid join thread to the controller.java and then increase or decrease the load size accordingly.
	
## Hybridjoin.java:
	This is the main file on which my whole project is based. This file receives the streamed data from the stream generator thread and load the tuples in the Multihashmap along with it, I am maintaining a queue which keep record of the joining attributes i.e., Productid in my case. Each node in the queue is pointing to its corresponding tuple in the Multihashtable. Then I load a chunk of master data using the buffered reader provided in java. This chunk of master data is loaded based on the 10 oldest node and if they match with the join attribute of the master data tuple then I merger both the tuples i.e., the transaction tuple from Multihashmap and the received master data tuple from the buffered reader. After merging, the tuples are added to the data warehouse. The matched tuples are then removed from the Multihashmap along with there join attribute in the queue and the variable tuplesprocessed is incremented which was sent to the controller thread. 

	Also in this thread, I am doing preprocessing with the date format. This preprocessing includes extracting the date with the standard format and the month, quarter and is_weekend using the java. time library.

	The function insertintoDW () is responsible for all the data insertion queries. It calls the getDateAttributes (date received) and inserts the data accordingly. Also I am checking for duplicate order IDs in the function and not inserting the duplicates because each transaction should have a unique ID. The function isOrderIDExists () is responsible for this.

	Moreover many functions are implemented. For instance, the getters of the join attribute, Loader function for the master data and received streamed data etc.


