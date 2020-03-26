import tweepy
from tweepy import OAuthHandler
from config import *
from tweepy import Stream
from listener import CosmosDBListener

import azure.cosmos
from azure.cosmos import CosmosClient,PartitionKey

import datetime

if __name__ == '__main__':    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    client = CosmosClient(host, masterKey)
    dblink = client.create_database_if_not_exists(id="Twitter")
    collLink = dblink.create_container_if_not_exists(   
        id="Tweets",
        partition_key = PartitionKey(path="/id"),
        offer_throughput = 1000
    )

    twitter_stream = Stream(auth, CosmosDBListener(client, collLink))
    twitter_stream.filter(track=['#CosmosDB', '#ApacheSpark', '#ChangeFeed', 'ChangeFeed', '#MachineLearning', '#BigData', '#DataScience', '#Mongo', '#Graph'], is_async=False)