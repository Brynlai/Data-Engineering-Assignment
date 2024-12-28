"""
Author: Lai ZhonPoa
"""

import requests
from bs4 import BeautifulSoup
from typing import List, Optional

class ForumScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def scrape_comments(self, soup: BeautifulSoup, article_id: int) -> List[tuple]:
        """
        Scrapes all comments from a given BeautifulSoup object of an article page.

        Args:
            soup (BeautifulSoup): BeautifulSoup object representing the article page.
            article_id (int): Article ID.

        Returns:
            List[tuple]: A list of tuples, where each tuple represents a comment
                         and contains (article_id, comment_id, user, comment_text).
                         Returns an empty list if no comments are found.
        """
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

            comments.append((article_id, comment_id, user, comment_text))

        return comments

    def scrape_article(self, url: str, article_id: int) -> Optional[tuple]:
        """
        Scrapes article details and comments from a given URL.

        Args:
            url (str): URL of the article.
            article_id (int): Article ID.

        Returns:
            Optional[tuple]: A tuple containing (article_id, title, date, publisher, views, comments_count, content, comments).
                             Returns None if an error occurs during scraping.
        """
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('title')
            title = title_tag.text.strip() if title_tag else "Unknown"
            date_tag = soup.find('p', class_='xg1')
            date = date_tag.text.split('|')[0].strip() if date_tag else "Unknown"
            publisher_tag = date_tag.find('a') if date_tag else None
            publisher = publisher_tag.text.strip() if publisher_tag else "Unknown"
            views_tag = soup.find('em', id='_viewnum')
            views_str = views_tag.text.replace(',', '') if views_tag else "0"
            views = int(views_str) if views_str.isdigit() else 0
            comments_tag = soup.find('em', id='_commentnum')
            comments_count_str = comments_tag.text.replace(',', '') if comments_tag else "0"
            comments_count = int(comments_count_str) if comments_count_str.isdigit() else 0
            content_tag = soup.find('td', id='article_content')
            content = content_tag.get_text(strip=True) if content_tag else ""
            comments = self.scrape_comments(soup, article_id)
            return (article_id, title, date, publisher, views, comments_count, content, comments)
        except Exception as e:
            print(f"Error scraping article ID {article_id}: {e}")
            return None

    def scrape_data(self, article_id: int) -> Optional[tuple]:
        """
        Scrapes article data and comments given an article ID.

        Args:
            article_id (int): Article ID.

        Returns:
            Optional[tuple]: A tuple containing two elements:
                               - A tuple with article details (article_id, title, date, publisher, views, comments_count, content).
                               - A list of comment tuples.
                             Returns None if an error occurs during scraping.
        """
        url = f"{self.base_url}{article_id}"
        try:
            print(f"Scraping article ID: {article_id}")
            scraped_data = self.scrape_article(url, article_id)
            if scraped_data:
                article = scraped_data[:-1]
                comments = scraped_data[-1]
                return article, comments
            return None
        except Exception as e:
            print(f"Error scraping article ID {article_id}: {e}")
            return None

# Example usage:
# scraper = ForumScraper(base_url="https://b.cari.com.my/portal.php?mod=view&aid=")
# article_data = scraper.scrape_data(aid=123)
# print(article_data)