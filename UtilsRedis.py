import redis

class Redis_Update_Count:
    valid_tatabahasa = [
        "kata nama", "kata kerja", "kata adjektif", "kata sendi", "kata keterangan", "tidak diketahui",
        "kata singkatan", "kata nama khas", "kata sendi nama", "kata ganti nama diri", "kata nama jamak",
        "kata ganti nama", "kata kerja pasif", "kata seru", "kata sifat", "kata ganti nama diri tunggal",
        "singkatan", "kata ganti", "kata nama waktu", "kata tanya", "kata hubung", "kata tunjuk",
        "nombor", "kata keternagan masa", "kata nombor"
    ]

    def __init__(self, host="localhost", port=6379, db=0):
        """
        Initialize the Redis connection.
        Args:
            host (str): Redis server host.
            port (int): Redis server port.
            db (int): Redis database number.
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def update_tatabahasa_count(self, tatabahasa):
        """
        Increment the count of a specific tatabahasa type in Redis after validation.
        Args:
            tatabahasa (str): The grammatical category to increment.
        """
        if tatabahasa not in self.valid_tatabahasa:
            tatabahasa = "tidak diketahui"  # Fallback for invalid tatabahasa types

        #redis best practice naming btw
        redis_key = "tatabahasa:counts"
        self.redis_client.hincrby(redis_key, tatabahasa, 1)

    def update_sentiment_count(self, sentiment):
        """
        Increment the count of sentiment distribution (positive, neutral, negative) in Redis.
        Args:
            sentiment (float): Sentiment score to categorize and increment.
        """
        redis_key = "sentiment:counts"
        if sentiment > 0:
            self.redis_client.hincrby(redis_key, "positive", 1)
        elif sentiment < 0:
            self.redis_client.hincrby(redis_key, "negative", 1)
        else:
            self.redis_client.hincrby(redis_key, "neutral", 1)

    def store_sentiment(self, word, sentiment):
        """
        Store or update the sentiment value for a specific word in Redis.
        Args:
            word (str): The word key to store the sentiment.
            sentiment (float): The sentiment score.
        """
        redis_key = f"sentiment:{word}"
        self.redis_client.hset(redis_key, mapping={"sentiment": sentiment})

    def get_sentiment(self, word):
        redis_key = f"sentiment:{word}"
        return self.redis_client.hgetall(redis_key)

    def close(self):
        """
        Close the Redis connection.
        """
        self.redis_client.close()
