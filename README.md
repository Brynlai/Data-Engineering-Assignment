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
- **Where to Store**: Local files (CSV/JSON) or a visualization platform (e.g., Tableau).  
- **Tool**: PySpark for analytics, visualization tools for reporting.  

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
