import tweepy
import logging
from collections.abc import Callable
from datetime import datetime
from config import settings
from src.database.models import Tweet
from src.database.database import SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TweetStreamListener(tweepy.StreamingClient):
    def __init__(
        self,
        bearer_token: str,
        callback: Callable | None = None,
        save_to_db: bool = True,
        **kwargs
    ):
        super().__init__(bearer_token, **kwargs)
        self.callback = callback
        self.save_to_db = save_to_db
        self.tweet_count = 0
        
    def on_tweet(self, tweet):
        try:
            tweet_data = {
                'tweet_id': str(tweet.id),
                'text': tweet.text,
                'created_at': datetime.utcnow(),
                'author_id': str(tweet.author_id) if hasattr(tweet, 'author_id') else None,
                'lang': tweet.lang if hasattr(tweet, 'lang') else None,
            }
            
            self.tweet_count += 1
            logger.info(f"Received tweet #{self.tweet_count}: {tweet_data['tweet_id']}")
            
            if self.save_to_db:
                self._save_to_database(tweet_data)
            
            if self.callback:
                self.callback(tweet_data)
                
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
    
    def _save_to_database(self, tweet_data: dict):
        db = SessionLocal()
        try:
            tweet = Tweet(**tweet_data)
            db.add(tweet)
            db.commit()
            logger.debug(f"Saved tweet {tweet_data['tweet_id']} to database")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            db.rollback()
        finally:
            db.close()
    
    def on_errors(self, errors):
        logger.error(f"Stream errors: {errors}")
        return True


class TwitterStreamManager:
    def __init__(self):
        self.stream = None
        self.rules = []
        
    def setup_stream(
        self,
        callback: Callable | None = None,
        save_to_db: bool = True
    ) -> TweetStreamListener:
        self.stream = TweetStreamListener(
            bearer_token=settings.twitter_bearer_token,
            callback=callback,
            save_to_db=save_to_db
        )
        return self.stream
    
    def add_rules(self, keywords: list[str], tag: str | None = None):
        rules = []
        for keyword in keywords:
            rule_value = f"{keyword} lang:en -is:retweet"
            rule = tweepy.StreamRule(value=rule_value, tag=tag or keyword)
            rules.append(rule)
        
        if self.stream:
            try:
                self.stream.add_rules(rules)
                logger.info(f"Added {len(rules)} rules to stream")
                self.rules.extend(rules)
            except Exception as e:
                logger.error(f"Error adding rules: {e}")
    
    def clear_rules(self):
        if self.stream:
            try:
                existing_rules = self.stream.get_rules()
                if existing_rules.data:
                    rule_ids = [rule.id for rule in existing_rules.data]
                    self.stream.delete_rules(rule_ids)
                    logger.info(f"Cleared {len(rule_ids)} existing rules")
                    self.rules = []
            except Exception as e:
                logger.error(f"Error clearing rules: {e}")
    
    def start_stream(
        self,
        keywords: list[str],
        callback: Callable | None = None,
        save_to_db: bool = True,
        clear_existing: bool = True
    ):
        if not self.stream:
            self.setup_stream(callback=callback, save_to_db=save_to_db)
        
        if clear_existing:
            self.clear_rules()
        
        self.add_rules(keywords)
        
        logger.info("Starting Twitter stream...")
        self.stream.filter(
            tweet_fields=['author_id', 'created_at', 'lang', 'public_metrics'],
            expansions=['author_id']
        )
    
    def stop_stream(self):
        if self.stream:
            self.stream.disconnect()
            logger.info("Stream stopped")


def create_batch_collector(batch_size: int = 100):
    batch = []
    
    def collect_batch(tweet_data: dict):
        batch.append(tweet_data)
        if len(batch) >= batch_size:
            logger.info(f"Batch of {len(batch)} tweets ready for processing")
            processed_batch = batch.copy()
            batch.clear()
            return processed_batch
        return None
    
    return collect_batch
