import tweepy
from tweepy import OAuthHandler
from azure.cosmos import CosmosClient,PartitionKey

from config import *
import json

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

client = CosmosClient(host, masterKey)
database = client.create_database_if_not_exists(id="sample")
container_name = "familycontainer"
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key = PartitionKey(path="/id"),
    offer_throughput = 400
)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    dictdata = json.loads(tweet)
    dictdata["id"] = str(dictdata["id"])
    container.create_item(dictdata)

