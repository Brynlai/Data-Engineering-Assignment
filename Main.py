import requests
from bs4 import BeautifulSoup
from typing import List

# Get from other files:
from Scrape_dbp import fetch_definition
from Classes import Scraped_Data, Comment

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
def scrape_article(url: str, aid: int) -> Scraped_Data:
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
    
        comments = scrape_comments(soup)
    
        return Scraped_Data(aid, title, date, publisher, views, comments_count, content, comments)
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


from Scrape_dbp import fetch_definition
from Classes import Scraped_Data, Comment


def main():
    base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
    aid_values = [4]
    unique_words_set = set() # Sets cannot contain duplicates

    for aid in aid_values:
        url = f"{base_url}{aid}"
        print(f"!-- Scraping AID: {aid}")
        scraped_data = scrape_article(url, aid)
        
        if scraped_data:
            all_words = scraped_data.get_all_text()
            unique_words_set.update(all_words)

    unique_words_list = list(unique_words_set)
    print(f"\nUnique Words head(5): {unique_words_list[:5]}")
    for word in unique_words_list:
        result = fetch_definition(word)
        definition = result[1] if result and result[1] else f"{word} not found"
        print(f"\nDefinition of {word}: {definition}")
        
    print("Successfully ended")
if __name__ == "__main__":
    main()
