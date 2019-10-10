#!/bin/bash
echo '["https://190.2.153.131:38239", "https://190.2.153.131:38240", "https://190.2.153.131:38241", "https://190.2.153.131:38242", "https://190.2.153.131:38243", "https://190.2.153.131:38244", "https://190.2.153.131:38301", "https://190.2.153.131:38302", "https://190.2.153.131:38303", "https://190.2.153.131:38304"]' > './proxies.json'
screen -dmS TRAFFICBOT-01 /usr/bin/python3 traffic_bot.py 1
sleep 3
screen -dmS TRAFFICBOT-02 /usr/bin/python3 traffic_bot.py 2
sleep 3
screen -dmS TRAFFICBOT-03 /usr/bin/python3 traffic_bot.py 3
sleep 3
screen -dmS TRAFFICBOT-04 /usr/bin/python3 traffic_bot.py 4
sleep 3
screen -dmS TRAFFICBOT-05 /usr/bin/python3 traffic_bot.py 5
