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
            #self.redis = redis.Redis(host=host, port=port, db=db, username=username, password=password, decode_responses=True)
            self.redis = redis.Redis(
                    host='redis-17304.c56.east-us.azure.redns.redis-cloud.com',
                    port=17304,
                    password='FZIVu26nMThUk6Bvkco1k9A45EhaFMIo')
            
            print("Connected to Redis=",self.redis.ping())
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
                for line in infile:
                    elements = line.strip().split('" "')
                    user_id = elements[0].split(':')[1].strip('"')
                    print("elements:",elements)
                    print("user_id:",user_id)
                    print("user str:", elements[0].split(':')[0].strip('"'))
                    print( "elements 1:", elements[1].strip('"'))
                    print( "elements 2:", elements[2].strip('"'))
 
                    user_data = {
                        elements[0].split(':')[0].strip('"'): elements[0].split(':')[1].strip('"'),
                        elements[1].strip('"'): elements[2].strip('"'),
                        elements[3].strip('"'): elements[4].strip('"'),
                        elements[5].strip('"'): elements[6].strip('"'),
                        elements[7].strip('"'): elements[8].strip('"'),
                        elements[9].strip('"'): elements[10].strip('"'),
                        elements[11].strip('"'): elements[12].strip('"'),
                        elements[13].strip('"'): elements[14].strip('"'),
                        elements[15].strip('"'): elements[16].strip('"'),
                        elements[17].strip('"'): elements[18].strip('"'),
                        elements[19].strip('"'): elements[20].strip('"'),
                        elements[21].strip('"'): elements[22].strip('"')
                    }
                    print("user_data:", user_data)
                    self.redis.hset(f"user:{user_id}", mapping=user_data)
                    print("user_data posted for ", user_id)
            result = 1
            print("Users data loaded successfully.")
        except Exception as e:
            print("Error loading users data:", e)
            print_stack()
        return result

    def load_scores(self, file):
        result = 0
        pipe = self.redis.pipeline()
        try:
            with open(file, mode='r') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    pipe.zadd('leaderboard:2', {row['user:id']: float(row['score'])})
            result = pipe.execute()
            print("Scores data loaded successfully.")
        except Exception as e:
            print("Error loading scores data:", e)
            print_stack()
        return result

    def query1(self, usr):
        try:
            user_attributes = self.redis.hgetall(f"user:{usr}")
            #print("Query 1 Result:", user_attributes)
            user_attributes = {key.decode(): value.decode() for key, value in user_attributes.items()}
            print(f"Attributes for user {usr}: {user_attributes}")
            return user_attributes
        except Exception as e:
            print("Error executing query1:", e)
            print_stack()

    def query2(self, usr):
        try:
            longitude = self.redis.hget(f"user:{usr}", "longitude")
            latitude = self.redis.hget(f"user:{usr}", "latitude")
            coordinates = (longitude.decode("utf-8"), latitude.decode("utf-8"))
            print(f"Query 2 : Coordinate of user {usr}:", coordinates)
            return coordinates
        except Exception as e:
            print("Error executing query2:", e)
            print_stack()


    def query3(self):
        try:
            result_keys = []
            result_lastnames = []

            # Use the SCAN command with a pattern to retrieve user keys
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match='user:*')
                for key in keys:
                    user_id = key.decode().split(':')[1]
                    if int(user_id[0]) % 2 == 0:
                        result_keys.append(key.decode("utf-8"))
                        lastname = self.redis.hget(key, "last_name")
                        result_lastnames.append(lastname.decode("utf-8"))

                # Break the loop when cursor is 0 (no more keys)
                if cursor == 0:
                    break

            print("Query 3: User IDs:", result_keys)
            print("Query 3: Last Names:", result_lastnames)
            return result_keys, result_lastnames
        except Exception as e:
            print("Error executing optimized query3:", e)
            print_stack()



    def query4(self):
        try:
            index_name = "user_index"
            self.redis.ft(index_name).create_index([
                TextField("gender"), TagField("country"), NumericField("latitude"),
                TextField("first_name")
            ], definition=IndexDefinition(prefix=["user:"]))

            query = Query("@gender:Female @country:{China|Russia} @latitude:[40 46]").paging(0, 100)
            result = self.redis.ft(index_name).search(query)
            result_docs = [doc.id for doc in result.docs]
            print("Query 4: female in China or Russia with a latitude between 40 and 46:", result_docs)
            return result_docs
        except Exception as e:
            print("Error executing query4:", e)
            print_stack()

    def query5(self):
        try:
            top_players = self.redis.zrevrange('leaderboard:2', 0, 9, withscores=True)
            email_ids = []
            for player in top_players:
                user_id = player[0].decode()
                email_id = self.redis.hget(f"{user_id}", "email")
                email_ids.append(email_id.decode("utf-8"))
            print("Query 5 : Email ids of the top 10 players:", email_ids)
            return email_ids
        except Exception as e:
            print("Error executing query5:", e)
            print_stack()

# Example usage:
rs = Redis_Client()
rs.connect()
rs.load_users("C:\\Data\\Assig-4\\users.txt")
rs.load_scores("C:\\Data\\Assig-4\\userscores.csv")
rs.query1(299)
rs.query2(2836)
rs.query3()
rs.query3()
rs.query4()
rs.query5()