import time
from datetime import datetime
from threading import RLock


INSERTION_LOCK_STREAM = RLock()
INSERTION_LOCK_SESSIONS = RLock()


def time_to_db_time(any_time=None):
    if not any_time:
        any_time = time.time()
    db_time_fmt = "%Y-%m-%d %H:%M:%S"
    if isinstance(any_time, str):
        if any_time[-1] == "Z":
            dt_object = datetime.strptime(any_time, "%Y-%m-%dT%H:%M:%SZ")
            return dt_object.strftime(db_time_fmt)
    elif isinstance(any_time, float):
        dt_object = datetime.fromtimestamp(any_time)
        return dt_object.strftime(db_time_fmt)
    elif isinstance(any_time, int):
        dt_object = datetime.fromtimestamp(any_time)
        return dt_object.strftime(db_time_fmt)


def get_all_websites_sql(pool):
    with pool.get_connection() as curs:
        curs.execute(
            "SELECT id, domain FROM websites "
            "WHERE enabled = 1"
        )
        output = curs.fetchall()
        return [i for i in output]


def get_all_ref_sites(pool):
    with pool.get_connection() as curs:
        curs.execute(
            "SELECT id, domain FROM websites "
            "WHERE enabled = 1"
        )
        return curs.fetchall()


def get_website_settings_sql(pool):
    with pool.get_connection() as curs:
        curs.execute(
                f'SELECT w.enabled, w.domain, site_id,ad_clicks_enabled,percent_ctr,'
                f'global_site_weight, percent_twitter_refs,percent_btctalk_refs,percent_google_refs,'
                f'percent_mobile_users, percent_alexa_tool '
                f'FROM site_settings JOIN websites w on site_settings.site_id = w.id '
                f'AND enabled = 1'
        )
        return {
            r[1]: {
                'site_id': int(r[2]),
                'ad_clicks_enabled': True if r[3] == 1 else False,
                'percent_ctr': round(float(r[4]), 2),
                'global_site_weight': round(float(r[5]), 2),
                'percent_twitter_refs': round(float(r[6]), 2),
                'percent_btctalk_refs': round(float(r[7]), 2),
                'percent_google_refs': round(float(r[8]), 2),
                'percent_mobile_users': round(float(r[9]), 2),
                'percent_alexa_tool': round(float(r[10]), 2),

            } for r in curs.fetchall()
        }


def get_website_locators_sql(pool, website):
    with pool.get_connection() as curs:
        curs.execute(
                f'SELECT element_locator FROM website_locators '
                f'JOIN websites web on website_locators.site_id = web.id '
                f'AND domain = %s', website
        )
        output = curs.fetchall()
        return [i[0] for i in output]


def get_referrer_links_sql(pool, website, ref_type):
    with pool.get_connection() as curs:
        curs.execute(
            f'SELECT url, element_locator, alt_locator FROM referrer_links '
            f'JOIN websites w on referrer_links.site = w.id '
            f'JOIN referrer_sites rs on referrer_links.ref_site_id = rs.id '
            f'AND domain = %s AND name = %s', (website, ref_type,)
        )
        return [i for i in curs.fetchall()]


def get_all_proxies_and_worker_ids_sql(pool):
    with pool.get_connection() as curs:
        curs.execute(
            f'SELECT id, proxy FROM bots_proxies'
        )
        return [i for i in curs.fetchall()]


def update_bot_sessions_start_sql(pool, bot_id, site, ref_id, proxy, locale, ext_ip, country,
                                  language, user_agent, alexa, mobile, banner):
    with INSERTION_LOCK_SESSIONS:
        with pool.get_connection() as curs:
            curs.execute(
                "INSERT INTO bot_sessions (bot_id, site, ref_id, proxy_address, locale, external_ip, country, language, user_agent, alexa_installed, mobile_traffic, clicked_banner, time_started) "
                "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, QUOTE(%s), %s, %s, %s, %s );", (
                    int(bot_id), str(site), int(ref_id), proxy, locale, ext_ip, country,
                    language, str(user_agent), int(alexa), int(mobile), int(banner), time_to_db_time(time.time()))

            )
            curs.execute(
                "SELECT LAST_INSERT_ID();"
            )
            return curs.fetchone()[0]


def update_bot_sessions_finish_sql(pool, cookies, clicked_banner, last_inserted_id):
    with INSERTION_LOCK_SESSIONS:
        with pool.get_connection() as curs:
            curs.execute(
                "UPDATE bot_sessions "
                "SET cookies = %s, clicked_banner = %s, time_finished = %s "
                "WHERE id = %s", (str(cookies), int(clicked_banner), time_to_db_time(), int(last_inserted_id))
            )
    return True


def update_traffic_stream(pool, session_id, url):
    with INSERTION_LOCK_STREAM:
        with pool.get_connection() as curs:
            curs.execute(
                f'INSERT INTO impressions(session_id, url, time_loaded) '
                f'VALUES (%s, %s, %s)', (session_id, url, time_to_db_time())
            )
    return True


def proxy_ext_ip_was_used(pool, ext_ip):
    match = (ext_ip,)
    with pool.get_connection() as curs:
        curs.execute(
            f'SELECT external_ip FROM bot_sessions ORDER BY id DESC LIMIT 10'
        )
        result = curs.fetchall()
    if match in result:
        return True
    return False


def get_geoip_info_sql(pool, proxy, ext_ip):
    with pool.get_connection() as curs:
        curs.execute(
            f'SELECT country, language, user_agent, cookies, mobile_traffic, id, screen_x, screen_y FROM bot_sessions '
            f'WHERE proxy_address = %s AND external_ip = %s  ORDER BY id DESC LIMIT 10', (proxy, ext_ip)
        )
        result = curs.fetchall()[0]
    return {
        'country': result[0],
        'language': result[1],
        'user_agent': result[2][1:-1] if result[2] else None,
        'cookies': result[3],
        'is_mobile': True if result[4] is 1 else False,
        'session_id': result[5],
        'screen_size': (result[6], result[7])
    }
