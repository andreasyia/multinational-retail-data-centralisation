# **Multinational-Retail-Data-Centralisation**
This project aims to improve the data-driven decision-making process by centralizing sales data. Currently, the sales data is scattered across multiple sources, making it challenging to access and analyze. The main scope is to develop a system that stores all the company's sales data in a centralized database. Once the centralized database is set up, you will be able to query it to obtain up-to-date metrics for the business. In summary, this enables the company to have a single source of truth and facilitates data-driven decision-making processes.

### **Milestone 1**
A repository was created in GitHub to track the changes in the code. 

### **Milestone 2**
A database was created in pgadmin4 named `sales_data`. Four Python scripts were created and named `data_extraction.py`, `database_utils.py`, `data_cleaning.py` and `main.py`. Each one of these has classes named `DataExtractor`, `DatabaseConnector`, and `DataCleaning` respectively. In `main.py` there are instances of each class and functions that are used to call methods from every class. For each class, several methods were created to access, retrieve and clean the data. Also, the necessary credentials were given along with the endpoints and links in order to connect and retrieve the data. After data was extracted and cleaned, data was uploaded to the database `sales_data` in pgadmin4.

### **Milestone 3**
Most of the tasks in milestone_3 related to casting the table's columns in the database `sales_data`. The majority of data inserted in the `sales_data` were of type text. Many columns converted to more appropriate data types such as `VARCHAR`, `UUID`, `SMALINT`, etc. Additionally, the constraints `primary keys` and `foreign keys` have been added to the columns to enforce data integrity, facilitate data retrieval, and maintain the coherence of data within a relational database management system.

### **Milestone 4**

## **Conclusion**


