import requests
from bs4 import BeautifulSoup
from typing import List, Optional

class Comment:
   def __init__(self, comment_id: int, user: str, comment_text: str):
     self.comment_id = comment_id
     self.user = user
     self.comment_text = comment_text

   def __str__(self):
      return f"""
      Comment ID: {self.comment_id}
      User: {self.user}
      Comment:
      {self.comment_text}
      """


class ScrapedData:
    def __init__(self, 
                 aid: int, 
                 title: str, 
                 date: str, 
                 publisher: str, 
                 views: int, 
                 comments_count: int, 
                 content: str, 
                 comments: List[Comment]) -> None:
        self.aid = aid
        self.title = title
        self.date = date
        self.publisher = publisher
        self.views = views
        self.comments_count = comments_count
        self.content = content
        self.comments = comments

    def __str__(self) -> str:
        comments_str = "\n".join(str(comment) for comment in self.comments)
        return f"""
        Aid: {self.aid}
        Title: {self.title}
        Date: {self.date}
        Publisher: {self.publisher}
        Views: {self.views}
        Comments Count: {self.comments_count}
        Content:
        {self.content}
        Comments Section:
        {comments_str}
        """

# Function to scrape all comments
def scrape_comments(soup: BeautifulSoup) -> List[Comment]:
    comments: List[Comment] = []

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

        comments.append(Comment(comment_id=comment_id, user=user, comment_text=comment_text))

    return comments


# Scrape article from URL and aid
def scrape_article(url: str, aid: int) -> ScrapedData:
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

    comments = scrape_comments(soup)

    return ScrapedData(aid, title, date, publisher, views, comments_count, content, comments)

def main():
    base_url = "https://b.cari.com.my/portal.php?mod=view&aid="

    # List of aids to scrape
    aid_values = list(range(1,2))  # Should be until 25000+
    aid_values.append(20000) # Because AID is sequential.

    for aid in aid_values:
        url = f"{base_url}{aid}"   # Construct full URL

        try:
            scraped_data = scrape_article(url, aid)   # Scrape data

            print(scraped_data)                         # Print data

            print("\n------------------------\n")       # Print separator

        except Exception as e:
            print(f"Error scraping {url}: {e}")

main()
