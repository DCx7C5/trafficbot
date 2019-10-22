from pymysqlpool import ConnectionPool


def create_connection_pool(bot_id=None):
    return ConnectionPool(
        host='127.0.0.1',
        user='arnedevXY' if bot_id == 10 else f'trafficbot{bot_id}',
        password='hsv4ever!',
        db='trafficbot',
        autocommit=True,
        size=20,
        charset="utf8"
    )


database_connection_pool = create_connection_pool()
