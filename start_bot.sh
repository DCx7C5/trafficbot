#!/bin/bash
echo '["https://190.2.153.131:38239", "https://190.2.153.131:38240", "https://190.2.153.131:38241", "https://190.2.153.131:38242", "https://190.2.153.131:38243", "https://190.2.153.131:38244", "https://190.2.153.131:38301", "https://190.2.153.131:38302", "https://190.2.153.131:38303", "https://190.2.153.131:38304"]' > './proxies.json'
echo '{"trafficbot1": {"time": null, "last_proxy": null}, "trafficbot2": {"time": null, "last_proxy": null}, "trafficbot3": {"time": null, "last_proxy": null}, "trafficbot4": {"time": null, "last_proxy": null}, "trafficbot5": {"time": null, "last_proxy": null}, "trafficbot6": {"time": null, "last_proxy": null}, "trafficbot7": {"time": null, "last_proxy": null}, "trafficbot8": {"time": null, "last_proxy": null}, "trafficbot9": {"time": null, "last_proxy": null}, "trafficbot10": {"time": null, "last_proxy": null}}' > './keepalive.json'
screen -dmS TRAFFICBOT-01 /usr/bin/python3 traffic_bot.py 1
sleep 3
screen -dmS TRAFFICBOT-02 /usr/bin/python3 traffic_bot.py 2
sleep 3
screen -dmS TRAFFICBOT-03 /usr/bin/python3 traffic_bot.py 3
sleep 3
screen -dmS TRAFFICBOT-04 /usr/bin/python3 traffic_bot.py 4
sleep 3
screen -dmS TRAFFICBOT-05 /usr/bin/python3 traffic_bot.py 5
sleep 3
screen -dmS TRAFFICBOT-06 /usr/bin/python3 traffic_bot.py 6
sleep 3

screen -dmS BOTKEEPALIVE /usr/bin/python3 bots_alive_check.py