from typing import List
# Test
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


class Scraped_Data:
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

    def get_all_text(self) -> List[str]:
        # Combine title, content, and all comment texts into a single string
        all_text = f"{self.title} {self.content} "
        #for comment in self.comments:
            #all_text += f"{comment.comment_text} "
        
        # Split the combined text into words
        words = all_text.split()
        
        return words


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

    