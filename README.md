# Data Engineering Assignment

## Description
This project involves processing and analyzing scraped data using PySpark, Redis, and Neo4j. The aim is to store, process, and analyze text data efficiently.

## Usage

### Starting Services
0. Open Powershell in Administrator mode and run wsl:
    ```bash
    wsl ~
    ```
1. Start Hadoop and Spark services:
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```
2. Start Kafka and Zookeeper:
   > Note: Wait for about 30 seconds before performing the next step.
   ```bash
   zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
   kafka-server-start.sh $KAFKA_HOME/config/server.properties &
   ```
    

3. Switch to student:
    ```bash
    su - student
    ```
### Running Notebooks (Curently not working if ru scrape_article while consumer is running.)
1. Activate the virtual environment and start Jupyter Lab:
   ```bash
   source de-prj/de-venv/bin/activate
   jupyter lab
   ```
   
2. Open 2 Powershell Terminals from Windows, then go (de-venv) student@R2D3:~/urdirectory$
3. (To show kafka working) cd into the directory both files are in!
    - Producer Terminal:
       ```bash
       python kafka_producer_show.py
        ```
   - Consumer Terminal:
       ```bash
        python kafka_consumer_show.py
        ```
     > [!IMPORTANT]  
        DO NOT RUN 
        "$ python kafka_producer_show.py"
        when scrape_aritcles_into_words.ipynb or neo4j.ipynb is running.
        "kafka_consumer_show.py" can run in the background. 

4. Run the notebooks in this sequence:
   - `scrape_articles_into_words.ipynb`
   - `neo4j.ipynb`


### Stopping Services

1. Stop Kafka and Zookeeper:
   > Note: Wait for about 30 seconds before performing the next step.
   ```bash
   kafka-server-stop.sh
   zookeeper-server-stop.sh
   ```
3. Stop Hadoop and Spark services:
   ```bash
   stop-yarn.sh
   stop-dfs.sh
   ```


## Data Storage and Processing

### Data Collection and Raw Storage
- **What to Store**: Raw scraped text data.
- **Where to Store**: Hadoop HDFS.
- **Tool**: PySpark for ingestion and Hadoop for storage.

### Processed Data
- **What to Store**: Cleaned and tokenized text.
- **Where to Store**: Hadoop HDFS or a relational database.
- **Tool**: PySpark for preprocessing.

### Lexicon
- **What to Store**: Words with definitions, relationships, and POS annotations.
- **Where to Store**: Neo4j for relationships; Redis for fast retrieval.
- **Tool**: Neo4j and Redis.

### Analytics
- **What to Store**: Analytical results.
- **Where to Store**: Local files, Neo4j, and Redis.
- **Tool**: Neo4j.

### Real-Time Updates
- **What to Store**: New and updated words.
- **Where to Store**: Kafka for message streaming.
- **Tool**: Kafka and Spark Structured Streaming.

## Decision Highlights
- **Neo4j**: For storing and querying word relationships.
- **Redis**: For fast key-value lookups.
- **Hadoop HDFS**: For scalable storage of raw and processed data.
