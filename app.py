import tweepy
from kafka import KafkaProducer
import json
import configparser

class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, kafka_topic):
        super().__init__()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.kafka_topic = kafka_topic

    def on_status(self, status):
        message = {'tweet': status.text, 'created_at': str(status.created_at)}
        self.producer.send(self.kafka_topic, value=message)
        print(f"Tweet sent to Kafka: {message['tweet']}")

    def on_error(self, status_code):
        if status_code == 420:
            return False # disconnects the stream
        print(f"Encountered streaming error (status code: {status_code})")
        return True

if __name__ == '__main__':
    # Load Twitter API credentials
    config = configparser.ConfigParser()
    config.read('config/twitter_credentials.ini')  # Changed file name for clarity

    auth = tweepy.OAuthHandler(config['DEFAULT']['consumerKey'], config['DEFAULT']['consumerSecret'])
    auth.set_access_token(config['DEFAULT']['accessToken'], config['DEFAULT']['accessTokenSecret'])
    api = tweepy.API(auth)

    stream_listener = TwitterStreamListener(kafka_topic='iphone15_tweets')
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=['iPhone 15', 'Apple iPhone 15'], languages=['en'])
