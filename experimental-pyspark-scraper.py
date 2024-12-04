from pyspark.sql import SparkSession
from pyspark.sql.types import *
from bs4 import BeautifulSoup
import requests
from typing import Optional, List

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Distributed Web Scraper") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define schema for structured data
comment_schema = StructType([
    StructField("comment_id", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("comment_text", StringType(), True)
])

article_schema = StructType([
    StructField("aid", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("date", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("comments_count", IntegerType(), True),
    StructField("content", StringType(), True),
    StructField("comments", ArrayType(comment_schema), True)
])

# Function to scrape comments
def scrape_comments(soup: BeautifulSoup) -> List[dict]:
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

        comments.append({
            "comment_id": comment_id,
            "user": user,
            "comment_text": comment_text
        })

    return comments

# Function to scrape a single article
def scrape_article(aid: int) -> Optional[dict]:
    base_url = f"https://b.cari.com.my/portal.php?mod=view&aid={aid}"
    try:
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find('title').text if soup.find('title') else "Unknown"

        date_tag = soup.find('p', class_='xg1')
        date = date_tag.text.split('|')[0].strip() if date_tag else "Unknown"

        publisher_tag = date_tag.find('a') if date_tag else None
        publisher = publisher_tag.text if publisher_tag else "Unknown"

        views_tag = soup.find('em', id='_viewnum')
        views = int(views_tag.text.replace(',', '')) if views_tag else 0

        comments_tag = soup.find('em', id='_commentnum')
        comments_count = int(comments_tag.text.replace(',', '')) if comments_tag else 0

        content_tag = soup.find('td', id='article_content')
        content = content_tag.get_text(strip=False) if content_tag else ""

        comments = scrape_comments(soup)

        return {
            "aid": aid,
            "title": title,
            "date": date,
            "publisher": publisher,
            "views": views,
            "comments_count": comments_count,
            "content": content,
            "comments": comments
        }
    except Exception as e:
        print(f"Error scraping article {aid}: {e}")
        return None

# Main logic
def main():
    # Create RDD of article IDs
    aids = list(range(1, 100))  # Example AID range
    aid_rdd = spark.sparkContext.parallelize(aids)

    # Map scraping function across AIDs
    articles_rdd = aid_rdd.map(scrape_article).filter(lambda x: x is not None)

    # Convert to DataFrame
    articles_df = spark.createDataFrame(articles_rdd, schema=article_schema)

    # Write to HDFS
    articles_df.write.mode("overwrite").parquet("hdfs://localhost:9000/scraped_data/articles")

    print("Scraping and storage complete!")

if __name__ == "__main__":
    main()
