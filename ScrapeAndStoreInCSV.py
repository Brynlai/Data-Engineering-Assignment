from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests
from bs4 import BeautifulSoup
from typing import List
from Scrape_dbp import fetch_definition
from Classes import Scraped_Data, Comment


# Function to scrape all comments
def scrape_comments(soup: BeautifulSoup, aid: int) -> List[tuple]:
    comments = []

    comments_container = soup.find('div', id='comment_ul')
    if not comments_container:
        return comments

    comment_tags = comments_container.find_all(['dl', 'dI'], id=True)

    for comment_tag in comment_tags:
        comment_id_str = comment_tag.get('id', '').replace('comment_', '').split('_')[0]
        comment_id = int(comment_id_str) if comment_id_str.isdigit() else 0

        user_tag = comment_tag.find('a', class_='xi2')
        user = user_tag.text.strip() if user_tag else "Anonymous"

        comment_text_tag = comment_tag.find('dd')
        if comment_text_tag:
            quote_tags = comment_text_tag.find_all('div', class_='quote')
            for quote_tag in quote_tags:
                quote_tag.extract()
            comment_text = comment_text_tag.get_text(strip=True)
        else:
            comment_text = "No comment text"

        comments.append((aid, comment_id, user, comment_text))

    return comments


# Scrape article from URL and aid
def scrape_article(url: str, aid: int) -> tuple:
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
    
        title = str(soup.find('title').text if soup.find('title') else "Unknown")
    
        date_tag = soup.find('p', class_='xg1')
        date = str(date_tag.text.split('|')[0].strip()) if date_tag else "Unknown"
    
        publisher_tag = date_tag.find('a') if date_tag else None
        publisher = str(publisher_tag.text if publisher_tag else "Unknown")
    
        views_tag = soup.find('em', id='_viewnum')
        views_str = views_tag.text if views_tag else "0"
        views = int(views_str.replace(',', '')) if views_str.isdigit() else 0
    
        comments_tag = soup.find('em', id='_commentnum')
        comments_count_str = comments_tag.text if comments_tag else "0"
        comments_count = int(comments_count_str.replace(',', '')) if comments_count_str.isdigit() else 0
    
        content_tag = soup.find('td', id='article_content')
        content = str(content_tag.get_text(strip=False)) if content_tag else ""
    
        comments = scrape_comments(soup, aid)
    
        return (aid, title, date, publisher, views, comments_count, content, comments)
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

# Scraping and data processing
base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
aid_values = [4,5,6]

article_data = []
comments_data = []

for aid in aid_values:
    url = f"{base_url}{aid}"
    print(f"!-- Scraping AID: {aid}")
    scraped_data = scrape_article(url, aid)
    if scraped_data:
        article = scraped_data[:-1]  # Exclude comments
        comments = scraped_data[-1]  # Extract comments
        article_data.append(article)
        comments_data.extend(comments)

# Defining schemas
article_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Views", IntegerType(), True),
    StructField("Comments_Count", IntegerType(), True),
    StructField("Content", StringType(), True),
])

comments_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Comment_ID", IntegerType(), True),
    StructField("User", StringType(), True),
    StructField("Comment_Text", StringType(), True),
])

definitions_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Word", IntegerType(), True),
    StructField("Definition", IntegerType(), True),
    StructField("Antonym", IntegerType(), True),
    StructField("Synonym", IntegerType(), True),
    StructField("Tatabahasa", IntegerType(), True),
])

# Creating DataFrames
article_df = spark.createDataFrame(article_data, schema=article_schema)
comments_df = spark.createDataFrame(comments_data, schema=comments_schema)

# Displaying data
article_df.show()
comments_df.show()

# Saving to CSV
article_df.write.format("csv").mode("overwrite").option("header", "true").save("data/articles.csv")
comments_df.write.format("csv").mode("overwrite").option("header", "true").save("data/comments.csv")

# Saving to Parquet (optional)
article_df.write.mode("overwrite").save("data/articles.parquet")
comments_df.write.mode("overwrite").save("data/comments.parquet")

# Reading back CSV (verification)
articles_csv = spark.read.csv("data/articles.csv", header=True)
comments_csv = spark.read.csv("data/comments.csv", header=True)

articles_csv.show()
comments_csv.show()

spark.stop()
