import csv
import re
from traceback import print_stack
from pyparsing import Regex
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query
# https://redis.readthedocs.io/en/stable/examples.html
import sys
class Redis_Client():
    redis = None

    def __init__(self):
        self.redis = self.redis

    """
    Connect to redis with "host", "port", "db", "username" and "password".
    """
    def connect(self):
        host = "redis-17304.c56.east-us.azure.redns.redis-cloud.com"
        port = 17304
        db = "database-LXH5BGIG"
        username = "default"
        password = "FZIVu26nMThUk6Bvkco1k9A45EhaFMIo"
        try:
            self.redis = redis.Redis(host=host, port=port, db=db, username=username, password=password)
            print("Connected to Redis.")
        except Exception as e:
            print("Error connecting to Redis:", e)
            print_stack()

    """
    Load the users dataset into Redis DB.
    """
    def load_users(self, file):
        result = 0
        try:
            with open(file, mode='r', encoding="utf8") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    self.redis.hset(f"user:{row['id']}", mapping=row)
            result = 1
            print("Users data loaded successfully.")
        except Exception as e:
            print("Error loading users data:", e)
            print_stack()
        return result

    """
    Load the scores dataset into Redis DB.
    """
    def load_scores(self): #leaderboards for users
        pipe = self.redis.pipeline()
        #open and read from file
        # TODO:
        result = pipe.execute()
        print("load data for scores")
        return result

    # """
    # Delete all users in the DB.
    # """
    # def delete_users(self, hashes):
    # pipe = self.redis.pipeline()
    # for hash in hashes:
    # pipe.delete(hash)
    # result = pipe.execute()
    # return result
    # """
    # Erase everything in the DB.
    # """
    # def delete_all(self):
    # self.redis.flushdb()
    """
    Return all the attribute of the user by usr
    """
    def query1(self, usr):
        print("Executing query 1.")
        # TODO:
        # print(result)
        # return result

    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    """
    def query2(self, usr):
        print("Executing query 2.")
        #TODO:
        # print(coordinates)
        # return coordinates

    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    We want to search for a subset of keyspace with the cursor at 1280.
    To avoid the searching of the entire keyspace, we only want to go through only a small number of
    elements per call.
    That is, we expect to only search through the subset of the keyspace, and then incrementally iterate the
    next keyspace only if needed.
    (https://redis.io/commands/scan/). You can test the scan query in the redis-cli.
    """
    def query3(self):
        print("Executing query 3.")
        # TODO:Get the keys and last names of the users whose ids do not start with an odd number.
        # Searching for the keyspace start at cursor 1280.
        #print(userids, result_lastnames)
        #return userids, result_lastnames

    """
    Return the female in China or Russia with the latitude between 40 and 46.
    """
    def query4(self):
        print("Executing query 4.")
        # TODO: In order to query attributes other than the primary key, you need to first create a secondary
        # index in Redis with the following specification:
        # gender(text), country(tag), latitude(Numeric), first_name(text).
        # for doc in result.docs: # result is a returned object from redis search()
        # print(doc)
        # return result #returns a list of document objects

    """
    Get the email ids of the top 10 players(in terms of score) in leaderboard:2
    """
    def query5(self):
        print("Executing query 5.")
        #TODO:
        # print(result)
        # return result

# git@github.com:redis-developer/redis-datasets.git
rs = Redis_Client()
rs.connect()
rs.load_users("C:\\Data\\Assig-4\\users.txt")
#rs.load_scores()
rs.query1(299)
rs.query2(2836)
rs.query3()
rs.query4()
rs.query5()