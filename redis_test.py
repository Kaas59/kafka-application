import redis
import os

REDIS_HOST = os.environ['REDIS_HOST']

redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)

for key in redis_con.keys("metrics-log*"):
    # print(key)
    redis_con.delete(key)