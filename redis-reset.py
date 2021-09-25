import redis
import os
import sys
import ast
import time

MAX_PARTITIONS = 5
DIRECTORY = "data1" + "/"

def main():
    redis_con = redis.Redis(host=os.environ["REDIS_HOST"], port=6379, db=0)
    while True:
        print("=========================")
        print("1：キーのリストを取得")
        print("2：最後のスループット")
        print("3：削除処理")
        print("4：スループットを出力")
        print("99：停止")
        print("=========================")

        select_action = int(input("> "))

        if select_action == 1:
            print("キーのリストを取得します。")
            __get_key_list(redis_con)

        elif select_action == 2:
            print("スループットを集計します。")
            __cal_throughput(redis_con)

        elif select_action == 3:
            time_wait = 5
            print(str(time_wait)+"秒後に削除処理が実行されます。")
            for value in range(time_wait):
                print(time_wait - value)
                time.sleep(1)
            __reset_key(redis_con)

        elif select_action == 4:
            print("スループットを出力します。")
            __out_throughput(redis_con)

        elif select_action == 99:
            print("ログアウトします。")
            break
            
        else:
            print("存在しないキーです。")


def __reset_key(redis_con):
    res = redis_con.keys()
    print("削除リスト"+str([key.decode() for key in res ]))
    for key in [key.decode() for key in res ]:
        try:
            redis_con.delete(key)
        except:
            print("DELETE Error：" + str(key))

def __cal_throughput(redis_con):
    for value in range(5):
        try:
            a = "partition_"+str(value)
            res = redis_con.get(a)
            data = ast.literal_eval(res.decode())
            print(sum(data)/len(data))
        except :
            pass

def __get_key_list(redis_con):
    res = redis_con.keys()
    print([key.decode() for key in res ])

def __out_throughput(redis_con):

    path = ["./log/"+DIRECTORY+"log_"+str(index) for index in range(MAX_PARTITIONS)]

    while True:
        res = redis_con.keys()
        for key in [key.decode() for key in res ]:
            if key.startswith("log_"):
                data = redis_con.get(key)
                for index in range(MAX_PARTITIONS):
                    if key.startswith("log_"+str(index)):
                        __write_file(path[index], key, ast.literal_eval(data.decode()))
                        pass

                redis_con.delete(key)
                pass

            else:
                pass

def __write_file(path, key, data):
    with open(path, mode="a") as f:
        f.write("Key="+str(key)+"\n")
        f.write(str(data)+"\n")
        f.write("スループット："+str(sum(data)/len(data))+"\n\n")
        print(key+"スループット："+str(sum(data)/len(data)))
    pass            

if __name__=='__main__':
    main()

