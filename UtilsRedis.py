import redis

def save_word_frequencies_to_redis(redis_client, word_frequencies_df):
    """
    Save word frequencies to Redis hash.
    """
    for row in word_frequencies_df.collect():
        word = row["Cleaned_Word"]
        freq = row["Frequency"]
        redis_client.hset("word_frequencies", word, freq)
    print("Word frequencies successfully stored in Redis.")
