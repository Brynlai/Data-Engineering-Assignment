# Steps:
1. ```wsl ~```
2. Start: 
  ```
  start-dfs.sh
  ```
  ```
  start-yarn.sh
  ```
  ```
  zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
  ```
  Note: Wait for about 30 seconds before performing the next step.
  ```
  kafka-server-start.sh $KAFKA_HOME/config/server.properties &
  ```
  Note: Wait for about 30 seconds before performing the next step.

3. ```su - student```
4.  Python de-venv
  ```
  source de-prj/de-venv/bin/activate
  ```
5. ```jupyter lab```
6. Open 2 terminals in de-venv mode: (To show kafka working) In the directory both files are in!
   - Producer Terminal:
       - ```python kafka_producer_show.py```
   - Consumer Terminal:
       - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 kafka_consumer_show.py```

7. DO NOT RUN PRODUCER when scrape_aritcles_into_words.ipynb or neo4j.ipynb is running
8. Run scrape_articles_into_words.ipynb
9. Run neo4j.ipynb




## To set an env variable in a jupyter notebook, just use a % magic commands, either %env or %set_env, e.g.,
```
%env MY_VAR=MY_VALUE
```
(Use %env by itself to print out current environmental variables.)






# Stop: 
```
$ kafka-server-stop.sh
Note: Wait for about 30 seconds before performing the next step.

$ zookeeper-server-stop.sh
Note: Wait for about 30 seconds before performing the next step.

$ stop-yarn.sh
$ stop-dfs.sh
```


### **1. Data Collection and Raw Storage**  
- **What to Store**: Raw scraped text data.  
- **Where to Store**: Hadoop HDFS for scalable storage.  
- **Tool**: PySpark for ingestion and Hadoop for distributed storage.  

---

### **2. Processed Data**  
- **What to Store**: Cleaned and tokenized text (individual words, metadata like frequency).  
- **Where to Store**: Hadoop HDFS or a relational database (if structured).  
- **Tool**: PySpark for preprocessing.  

---

### **3. Lexicon**  
- **What to Store**:  
  - Words with their definitions, semantic relationships, and POS annotations.  
  - Sentiment scores for words.  
- **Where to Store**:  
  - **Neo4j**: For semantic relationships (synonyms, antonyms, hypernyms).  
  - **Redis**: For fast retrieval of word definitions or sentiment scores.  
- **Tool**: Neo4j for graph-based relationships; Redis for high-speed access.  

---

### **4. Analytics**  
- **What to Store**: Analytical results (e.g., word frequency, sentiment distribution).  
- **Where to Store**: Local files (CSV), Neo4J and Redis.
- **Tool**: Neo4J

---

### **5. Real-Time Updates**  
- **What to Store**: New incoming words, updates to existing words.  
- **Where to Store**: Kafka for message streaming.  
- **Tool**: Kafka for producer-consumer model; Spark Structured Streaming for processing.  

---

### **Decision Highlights**  
- **Neo4j**: Best for storing and querying word relationships.  
- **Redis**: Optimal for fast, key-value lookups (definitions, sentiments).  
- **Hadoop HDFS**: Reliable for raw and processed large-scale data.
