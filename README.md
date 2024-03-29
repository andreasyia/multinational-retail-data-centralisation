### **Multinational-Retail-Data-Centralisation**
This project aims to improve the data-driven decision-making process by centralizing sales data. Currently, the sales data is scattered across multiple sources, making it challenging to access and analyze. The main scope is to develop a system that stores all the company's sales data in a centralized database. Once the centralized database is set up, you will be able to query it to obtain up-to-date metrics for the business. In summary, this enables the company to have a single source of truth and facilitates data-driven decision-making processes.

#### **Project Structure**
A database was created in pgadmin4 named `sales_data`. Four Python scripts were created and named `data_extraction.py`, `database_utils.py`, `data_cleaning.py` and `main.py`. Each one of these has classes named `DataExtractor`, `DatabaseConnector`, and `DataCleaning` respectively. In `main.py` there are instances of each class and functions that are used to call methods from every class. For each class, several methods were created to access, retrieve and clean the data. Also, the necessary credentials were given along with the endpoints and links in order to connect and retrieve the data. After data was extracted and cleaned, data was uploaded to the database `sales_data` in pgadmin4.

#### **Creating the Star Schema Database**
Most of the tasks in milestone 3 related to casting the table's columns in the database `sales_data`. The majority of data inserted in the `sales_data` were of type text. Many columns converted to more appropriate data types such as `VARCHAR`, `UUID`, `SMALINT`, etc. Additionally, the constraints `primary keys` and `foreign keys` have been added to the columns to enforce data integrity, facilitate data retrieval, and maintain the coherence of data within a relational database management system.

#### **Business Analytics Scenarios using SQL**
In milestone 4, our primary objective was to leverage the power of data to drive better decision-making within the company and gain deeper insights into our sales performance. To achieve this, I have been assigned to answering critical business questions and extracting relevant data from the database `sales_data` using SQL. By employing SQL as our querying tool, we can access and manipulate the data stored in our database efficiently and effectively. This allowed us to generate valuable information and actionable insights, which will enable the company to make data-driven decisions with confidence and accuracy.

#### **Conclusion** 
In conclusion, this project has been a transformative learning experience, significantly enhancing my proficiency not only in utilizing the powerful data manipulation library, Pandas, but also in effectively applying Object-Oriented Programming (OOP) principles to create more efficient and maintainable code. Throughout the project, I have developed a strong foundation in designing well-structured classes and implementing diverse methods, resulting in a more organized and scalable codebase. Working extensively with Pandas has deepened my understanding of data cleaning and manipulation, enabling me to handle various data formats with ease and ensuring data integrity for accurate analysis. Moreover, the incorporation of SQL in the project has allowed me to leverage the power of querying the database, empowering the company to make data-driven decisions based on the insights derived from the data. This integration of SQL has provided valuable data-driven solutions to enhance the company's operations and strategic planning. The knowledge gained from this project will undoubtedly prove to be invaluable in my future endeavours, allowing me to contribute effectively to solving complex business problems and driving growth in data-intensive environments. I am excited to apply these skills and insights to continue making meaningful contributions to the success of the company and beyond.


#### <span style='text-decoration: underline; color: blue'>**Technologies Used**</span>

##### **Imported Modules**
- boto3
- getpass
- json
- numpy
- pandas
- re
- requests
- sqlalchemy
- tabula
- yaml

##### **Cloud Technologies**
- Amazon Web Services (AWS)

##### **Database Technologies**
1. Relational Database Management Systems (RDBMS)
   - PostgreSQL
2. Cloud Database
   - Amazon RDS (Relational Database Service)

##### **Programming and Querying Languages**
- Python
- SQL









