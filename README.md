# Multinational Retail Data Centralisation


## Objective of the Project
This is a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, my organisation would like to make its sales data accessible from one centralised location.

The first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

We will then query the database to get up-to-date metrics for the business.

![alt text](<Sales_data_schema_diagram.pgerd (1).png>)

### Step 1
To initialise database connection
To initialise the database connection, there is need to install YAML and SQLALCHEMY

### Step 2: 
Extract data using DataExtractor
extracting some of the data will require installing pandas, tabula, requests, boto3 to connect to aws s3 and AWS CLI, io and urlparser

### Step 3: 
Clean data using DataCleaning
Data cleaning also require pandas and once it has been installed in the previous steps, it should just be imported intot the data cleaning scripts

### Step 4: 
Upload cleaned data to database
The uploads took place inside the main.py file where all the other scripts were imported

## Observations

This project involves several key learnings especially in data handling, some of which are:

1. **Data Extraction**: Understanding how to extract data from various sources, such as databases (SQL), APIs, files (CSV, JSON, PDF), and cloud storage (S3).

2. **Data Cleaning**: Learning how to clean and preprocess data to ensure its quality and usability. This includes handling missing values, standardizing formats, and removing errors or outliers.

3. **Data Storage**: Setting up and managing databases (e.g., PostgreSQL) to store and organize large volumes of structured data effectively.

4. **Data Manipulation**: Performing data manipulation tasks using libraries like Pandas in Python, such as filtering, transforming, and aggregating data to derive insights.

5. **API Integration**: Understanding how to interact with external APIs to retrieve data programmatically, including authentication, sending requests, and handling responses.

6. **Error Handling**: Dealing with errors and exceptions that may occur during data processing, including debugging and troubleshooting issues to ensure smooth execution.

7. **Automated Processes**: Creating scripts and workflows to automate repetitive tasks in data handling, improving efficiency and reducing manual effort.

8. **Project Organization**: Structuring the project into modular components (classes, scripts) for better organization, readability, and maintainability.

9. **Collaboration**: Working collaboratively on a project with multiple components, potentially involving different team members with specialized skills.

10. **Problem-Solving**: Developing problem-solving skills to address challenges encountered during various stages of the project, from data extraction to storage and analysis.

Overall, this project provides me with valuable hands-on experience in data handling tasks, covering a wide range of concepts and skills essential for data professionals and analysts.

You can reach out if you have any question on this project. 
We will also be updating the scripts regularly if possible. I also intend to add data pipeline diagram this readme in the future.