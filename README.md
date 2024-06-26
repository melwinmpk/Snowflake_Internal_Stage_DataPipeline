# Snowflake_Internal_Stage_DataPipeline

<h2>Project Overview</h2>
<p>This project, titled "Snowflake Internal Stage Data Pipeline," focuses on developing a robust data ingestion pipeline from a MySQL database to Snowflake utilizing Snowflake's Internal Stage and orchestrated with Airflow. The pipeline is designed to perform incremental data loads, enhancing efficiency and reducing data transfer volumes. A metadata-driven approach is employed to streamline and automate the data ingestion process.</p>

## Youtube Video
<a href="https://www.youtube.com/watch?v=7hnOcUFgrWc" target="_blank"><img src="https://img.youtube.com/vi/7hnOcUFgrWc/0.jpg"></a>

<h2>Features</h2>
<ul>
<li><b>Incremental Data Loading:</b> Efficiently transfers only new or updated records from MySQL to Snowflake, minimizing data transfer and processing time.</li>
<li><b>Metadata-Driven Pipeline:</b> Leverages metadata to dynamically configure and execute data ingestion tasks, reducing the need for hard-coded configurations.</li>
<li><b>Airflow Orchestration:</b> Utilizes Apache Airflow to manage workflow orchestration, scheduling, and monitoring, ensuring reliable execution of data loading processes.</li>
<li><b>Snowflake Integration:</b> Employs Snowflake's Internal Stage for secure and scalable data staging before ingestion into the target tables.</li>
</ul>

<h2>Technologies Used</h2>
<ul>
<li><b>MySQL:</b> Source database for extracting data.</li>
<li><b>Snowflake:</b> Target cloud data warehouse for analytics.</li>
<li><b>Apache Airflow:</b> Workflow orchestration tool to manage the data pipeline.</li>
<li><b>Python:</b> The primary programming language for scripting and automation.</li>
</ul>

<!--<img width="1052" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/a3b2c949-2e49-4418-a7e2-bfede2356fc9">-->
<img width="1052" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/ccfbea03-5f73-41b0-a9ed-5b6eceb2a5c8">

<H3>Source Data</H3>
<p>There are 2 tables which are getting ingested to Snowflake.</p>
<p>Dedicated Dags are developed for each Table</p>
<ol>
	<li>amazone_books</li>
	<li>amazonebook_reviews </li>
</ol>
<h4>Source Table DDls</h4>
<pre>
CREATE TABLE amazone_books (
	book_id INT NOT NULL AUTO_INCREMENT
	,book_title TEXT
	,book_amount FLOAT
	,book_author TEXT
	,book_rating FLOAT
	,book_link TEXT
	,business_date DATE DEFAULT(CURRENT_DATE)
	,PRIMARY KEY (book_id)
	);

CREATE TABLE amazonebook_reviews (
	book_id INT NOT NULL
	,reviewer_name TEXT
	,rating FLOAT
	,review_title TEXT
	,review_content TEXT
	,reviewed_on DATE
 	,business_date DATE DEFAULT(CURRENT_DATE)
	);
</pre>
<p> For the Incremental load. Primary Keys are required in the Tables. Respective Primary key for the Table are</p>
<ul>
	<li>amazone_books</li>
	<ul>
		<li>book_id</li>
	</ul>	
	<li>amazonebook_reviews</li>
	<ul>
		<li>book_id</li>
		<li>reviewer_name</li>
		<li>business_date</li>
	</ul>
		
</ul>	
<p> Note: This Source Data is from another Project. To know more about how source data is generated please refer <a href='https://github.com/melwinmpk/AmazonBooks_DataPipeline?tab=readme-ov-file#amazonbookdata_datapipeline'>AmazonBooks_DataPipeline</a>  </p>
<p>The Airflow Dag Ids for respective Tables are</p>
<ul>
	<li>amazone_books</li>
	<ul>
		<li>Snowflake_InternalStage_amazone_books_Dag</li>
	</ul>	
	<li>amazonebook_reviews</li>
	<ul>
		<li>Snowflake_InternalStage_amazonebook_review_Dag</li>
	</ul>	
</ul>

<h3>How It Works</h3>
<ul>
<li><b>Workflow Design:</b> A DAG (Directed Acyclic Graph) in Airflow defines the sequence of tasks for data extraction, staging, and loading.</li>
<li><b>Incremental Loading:</b> The pipeline identifies new or updated records in MySQL using timestamps or sequence IDs.</li>
<li><b>Metadata Management:</b> Metadata definitions (i.e Config) guide the extraction and loading processes, allowing for flexibility and scalability.</li>
<li><b>Monitoring and Logging:</b> Airflow provides comprehensive monitoring and logging capabilities, facilitating troubleshooting and performance optimization.</li>
</ul>

<h3>Limitations and Future Enhancements</h3>
<ul>
	<li><b>Data Quality Checks:</b> Implement additional data validation and quality checks within the pipeline.</li>
	<li><b>Advanced Scheduling:</b> Enhance scheduling capabilities to support more complex dependencies and triggers.</li>
</ul>	


<h3>Screenshots for the Dag amazone_books</h3>
<img width="1210" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/f14d9492-fdc6-4670-b56b-75303751392e">
<img width="945" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/88cf3b74-3387-4e1e-9309-c550ee13d487">
<img width="361" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/51e9538e-0cd2-4c24-b5a2-45337524460d">
<img width="780" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/241fe857-f986-487b-98f1-e9a378b5b426">
<img width="782" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/71688582-4fc7-47ce-b646-c25e0c373463">
<img width="982" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/9db6ff6a-e755-4c33-97c2-bf9854b7b8ba">


<h3>Screenshots for the Dag amazonebook_reviews</h3>
<img width="1025" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/b60ce74f-6f63-45d7-8711-f7356dbdbb2f">
<img width="1027" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/a6972e16-6177-4c8e-8e17-edcdd2a92ad1">
<img width="1019" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/e11cb6ee-9d73-45c6-91d0-1cebc2f91c5d">
<img width="804" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/7c4060f9-1291-4e50-baee-dee350d2b979">
<img width="1219" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/d4c6c876-b711-4712-b2b2-22f3b5c38ab3">


<h3>Reference</h3>

<ul>
	<li><b>Incremental Load</b>: <a href="https://medium.com/@nripapathak/incremental-data-load-approach-and-implementation-strategy-dd89bc41629c" target="_blank">Medium</a></li>
	<li><b>Internal stage</b>:<a href="https://docs.snowflake.com/en/user-guide/data-load-local-file-system"  target="_blank">Snowflake Docs</a></li>
	<li><b>Power of Metadata driven ETL Frameworks</b>:<a href="https://www.linkedin.com/pulse/power-metadata-driven-etl-frameworks-dataleagueptyltd-3m1bc/
" target="_blank">Linkedin</a></li>
</ul>	
