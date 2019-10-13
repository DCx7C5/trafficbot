import os
import json
import subprocess
from time import sleep, time
from threading import Thread
from filelock import FileLock


CWD = os.getcwd()


def get_time_and_last_proxy(bot_id):
    with FileLock(f"{CWD}/.lock2"):
        with open(f'{os.getcwd()}/keepalive.json', 'r') as pj:
            data = json.load(pj)
        t = data[f'trafficbot{bot_id}']['time']
        lp = data[f'trafficbot{bot_id}']['last_proxy']
        with open(f'{os.getcwd()}/keepalive.json', 'w') as pj:
            pj.write(json.dumps(data))
    return t, lp


def put_proxy(proxy):
    with FileLock(f"{CWD}/.lock"):
        with open(f'{os.getcwd()}/proxies.json', 'r') as pj:
            data = json.load(pj)
        data.append(proxy)
        with open(f'{os.getcwd()}/proxies.json', 'w') as pj:
            pj.write(json.dumps(data))
    return True


def check_proxy(proxy):
    with FileLock(f"{CWD}/.lock"):
        with open(f'{os.getcwd()}/proxies.json', 'r') as pj:
            data = json.load(pj)
    if proxy in data:
        return True
    return False


class TrafficKeepAliveDaemon(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True

    def run(self) -> None:
        while True:
            sleep(360)
            for i in range(1, 7):
                t, last_proxy = get_time_and_last_proxy(bot_id=i)
                if time() > (t + 900):
                    print(f"Found dead TrafficBot0{i}")
                    try:
                        cmd_del = f"screen -X -S TRAFFICBOT-0{i} quit"
                        subprocess.check_call(cmd_del.split())
                    except:
                        pass
                    sleep(10)
                    if not check_proxy(last_proxy):
                        put_proxy(last_proxy)
                        print(f"Restored TrafficBot0{i} Proxy")
                    cmd_create = f"screen -dmS TRAFFICBOT-0{i} /usr/bin/python3 traffic_bot.py {i}"
                    subprocess.check_call(cmd_create.split())
                    print(f"Created new TrafficBot0{i}")
                sleep(2)


if __name__ == '__main__':
    print("STARTING CHECK SCRIPT")
    check_bot = TrafficKeepAliveDaemon()
    check_bot.start()
    check_bot.join()
