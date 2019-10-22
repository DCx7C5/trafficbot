from pymysqlpool import ConnectionPool


def create_connection_pool():
    return ConnectionPool(
        host='127.0.0.1',
        user='arnedevXY',
        password='hsv4ever!',
        db='trafficbot',
        autocommit=True,
        size=20,
        charset="utf8"
    )


database_connection_pool = create_connection_pool()
