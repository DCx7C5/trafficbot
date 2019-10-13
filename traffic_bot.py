import os
import sys
import time
import json
import logging
import requests
import coloredlogs
from multiprocessing import Process, Queue, Pipe, Lock
from socks import ProxyError
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from selenium.common.exceptions import WebDriverException, ElementNotInteractableException, JavascriptException
from selenium.webdriver import FirefoxProfile
from secrets import SystemRandom

from selenium.webdriver import Firefox
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.exceptions import InsecureRequestWarning
from user_agent import generate_user_agent
from selenium.webdriver.remote.remote_connection import LOGGER

from blocked import BLOCKED
from traffic_sql import get_website_settings_sql, update_bot_sessions_finish_sql, \
    update_bot_sessions_start_sql, get_referrer_links_sql, get_website_locators_sql, create_connection_pool, \
    proxy_ext_ip_was_used, get_geoip_info_sql




sec = SystemRandom()

LOGGER.setLevel(logging.WARNING)
CWD = os.getcwd()
pq = Queue()

logger = logging.getLogger('TRAFFICBOT')
urlliblog = logging.getLogger('urllib3')
sockslog = logging.getLogger('socks')
sockslog.setLevel(logging.WARNING)
urlliblog.setLevel(logging.CRITICAL)
coloredlogs.install(
    level=logging.INFO,
    fmt=f'%(asctime)-20s- %(name)-31s - %(process)-6s- %(levelname)-7s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

PROXIES = [
    "https://190.2.153.131:38239",
    "https://190.2.153.131:38240",
    "https://190.2.153.131:38241",
    "https://190.2.153.131:38242",
    "https://190.2.153.131:38243",
    "https://190.2.153.131:38244",
    "https://190.2.153.131:38301",
    "https://190.2.153.131:38302",
    "https://190.2.153.131:38303",
    "https://190.2.153.131:38304"
]
sec.shuffle(PROXIES)


def is_xpath_locator(locator_string: str) -> bool:
    """
    Checks if locator string is XPath Element
    """
    if locator_string.startswith("/"):
        return True
    return False


def calculate_chance_weight_based(data: [tuple]):
    result = []
    for x in data:
        for r in range(int(x[1] * 100)):
            result.append(x[0])
    return sec.choice(result)


def calculate_bool_percentage_based(data: float or int) -> bool:
    if (500 - data * 10 / 2) < sec.randint(0, 1000) < (500 + data * 10 / 2):
        return True
    return False


def calculate_chance_percentage_based(data) -> bool:
    result = []
    for x in data:
        for r in range(int(x[1] * 10)):
            result.append(x[0])
    return sec.choice(result)


def choose_fair(data):
    if isinstance(data, float) or isinstance(data, int):
        return calculate_bool_percentage_based(data)
    elif (isinstance(data, tuple) or isinstance(data, list)) and (isinstance(data[0], tuple) or isinstance(data[0], list)):
        return calculate_chance_weight_based(data)
    elif (isinstance(data, tuple) or isinstance(data, list)) and (not (isinstance(data[0], tuple) or isinstance(data[0], list))):
        return sec.choice(data)
    elif isinstance(data, str) and isinstance(float(data), float):
        return calculate_bool_percentage_based(data)


class DPP:
    """Daen Protocol Package"""
    def __init__(self, t: str, r: str, ty: str, data):
        self.FROM = t
        self.FROM_id_numeric = int(t[-1])
        self.TO = r
        self.TO_id_numeric = int(r[-1])
        self.TYPE = ty
        self.DATA = data


class TrafficBotProcess(Process):

    def __init__(self, bid: int, database_connection, communication_channel, log, lock_obj, headless=True):
        Process.__init__(self)
        self.id = bid
        self.name = 'Bot'
        self.logger = log.getChild(self.name)
        self.lock = lock_obj
        self.dpp_id = f'B{self.id}'
        self.com_chan = communication_channel
        self.db_conn = database_connection
        # Randomly choose site for this Process
        self.website_settings = get_website_settings_sql(self.db_conn)
        self.site = calculate_chance_weight_based(
            [(s, self.website_settings[s]["global_site_weight"]) for s in self.website_settings]
        )
        with self.lock:
            self.logger.info(self.site)

        self.clicks_ad = False
        self.clicked_ad = 0
        self.click_ad_allowed = self.website_settings[self.site]["ad_clicks_enabled"]

        if headless is True:
            os.environ['MOZ_HEADLESS'] = '1'

        self.proxy = None
        self.proxy_ip = None
        self.proxy_port = None
        self.active_ext_ip = None
        self.was_used = None
        self.info_dict = None
        self.active_country = None
        self.active_language = None
        self.active_google_domain = None
        self.active_ua = generate_user_agent(device_type='desktop')
        self.is_mobile = False
        self.error_counter = None
        self.rotation_time = None
        self.install_alexa_toolbar = None
        self.ref_id, self.ref = None, None
        self.session_id = None
        self.driver = None
        self.first_inited = None
        self.session_created = None
        self.default_window_handle = None

    def run(self) -> None:
        try:
            while not self.first_inited:
                self.first_init()
            while not self.session_created:
                self.create_session()
            self.ref()
            time.sleep(sec.randint(2, 8))
            self.handle_onsite()
        except ProxyError as pe:
            with self.lock:
                self.logger.error(str(pe))
        except Exception as ex:
            with self.lock:
                self.logger.error(str(ex))
        finally:
            self.save_session()
            self.lock.acquire()
            self.logger.debug('Session saved to database...')
            self.lock.release()
            self.close_and_quit()
            self.lock.acquire()
            self.logger.debug('Driver session closed and cleaned up...')
            self.logger.debug('Sending PXY Package to manager...')
            self.lock.release()
            self.com_chan.send(DPP(self.dpp_id, '0', 'PXY', self.proxy))
            self.lock.acquire()
            self.logger.debug('Sending END Package to instance, waiting for response...')
            self.lock.release()
            self.com_chan.send(DPP(self.dpp_id, f'P{self.id}', 'END', True))
            if self.com_chan.recv().DATA:
                self.lock.acquire()
                self.logger.debug('...received response, process stopping...')
                self.lock.release()

    def first_init(self):
        with self.lock:
            self.logger.debug("Requesting new proxy from TrafficBotManager")
        self.com_chan.send(DPP(self.dpp_id, '0', 'PXY', None))
        self.proxy = self.com_chan.recv().DATA
        with self.lock:
            self.logger.debug(f"Received proxy from TrafficBotManager: {self.proxy}")
        self.proxy_ip = self.proxy.split('//')[1].split(':')[0]
        self.proxy_port = int(self.proxy.split('//')[1].split(':')[1])

        self.active_ext_ip = self.get_ip_address_from_cmp()
        self.was_used = proxy_ext_ip_was_used(self.db_conn, self.active_ext_ip)

        if self.was_used:
            self.info_dict = get_geoip_info_sql(self.db_conn, self.proxy, self.active_ext_ip)
            self.active_country = self.info_dict['country']
            self.active_language = self.info_dict['language']
            self.active_google_domain = "https://www.google.com"
            self.active_ua = self.info_dict["user_agent"]
            self.is_mobile = self.info_dict["is_mobile"]

        else:
            self.info_dict = self.get_info_from_proxy_ip()
            self.active_country = self.info_dict['country']
            self.active_language = self.info_dict['language']
            self.active_google_domain = self.info_dict['google_domain']
            if choose_fair(self.website_settings[self.site]["percent_mobile_users"]):
                self.active_ua = generate_user_agent(device_type='smartphone')
                self.is_mobile = True
            else:
                self.active_ua = generate_user_agent(device_type='desktop')
                self.is_mobile = False

        self.error_counter = 0

        self.rotation_time = 300
        time.sleep(sec.randint(500, 3000) / 1000)
        # Create browser session

        self.ref_id, self.ref = choose_fair(
            [
                ((1, self.handle_google_ref), self.website_settings[self.site]['percent_google_refs']),
                ((2, self.handle_twitter_ref), self.website_settings[self.site]['percent_twitter_refs']),
                ((3, self.handle_bitcoin_talk_ref), self.website_settings[self.site]['percent_btctalk_refs']),
            ]
        )

        self.session_created = False

        # try Install Alexa sidebar plugin
        self.install_alexa_toolbar = False
        if choose_fair(self.website_settings[self.site]['percent_alexa_tool']):
            self.install_alexa_toolbar = True

        with self.lock:
            if self.was_used:
                self.logger.warning(f"Same external IP used in last 10 minutes: {self.proxy}")
            self.logger.debug(f"Fetched IP geolocation and meta data via {'database.' if self.was_used else 'HTTP request.'}")
            self.logger.debug(f"Socks5 proxy URL   : {self.proxy}")
            self.logger.debug(f"External IP address: {self.active_ext_ip}")
            self.logger.debug(f"Language code      : {self.active_language}")
            self.logger.debug(f"Country code       : {self.active_country}")
            self.logger.debug(f"Google domain      ; {self.active_google_domain}")
            self.logger.debug(f"User agent         : {self.active_ua}")
            self.logger.debug(f"Is mobile device   : {self.is_mobile}")
            self.logger.debug(f'Session website    ; {self.site}')
            self.logger.debug(f'Session refsite    : {self.ref_id}')
            self.logger.debug(f'Alexa toolbar      : {"Yes" if self.install_alexa_toolbar else "No"}')
            self.logger.debug(f'Ad click allowed   : {"Yes" if self.click_ad_allowed else "No"}')
        self.first_inited = True

    def create_session(self):
        profile = FirefoxProfile(f'{CWD}/firefox/profile/{"alexa" if self.install_alexa_toolbar else "user0"}')
        profile.set_preference(
            'intl.accept_languages', '{}en-US;q=0.9,en;q=0.8'.format(
                f'{self.active_language},' if self.active_language is not None else "en"
            ))

        profile.set_preference('general.useragent.override', self.active_ua)
        profile.set_preference('network.proxy.socks_remote_dns', True)
        profile.set_preference('network.proxy.socks_version', 5)
        profile.set_preference('network.proxy.socks_port', self.proxy_port)
        profile.set_preference('network.proxy.socks', f'{self.proxy_ip}')
        profile.set_preference('network.proxy.type', 1)
        profile.set_preference('network.proxy.no_proxies_on', ",".join(BLOCKED))
        profile.update_preferences()
        # Gecko driver log level

        self.driver = Firefox(
            executable_path=f"{CWD}/firefox/driver/geckodriver",
            firefox_binary=f"{CWD}/firefox/binary/firefox-bin",
            firefox_profile=profile,
        )
        if self.was_used and self.info_dict['cookies']:
            cookie_list = json.loads(self.info_dict['cookies'])
            for cookie in cookie_list:
                try:
                    self.driver.add_cookie(cookie)
                except:
                    with self.lock:
                        self.logger.warning(f"Failed import cookie for domain: {cookie['domain']}")
                    pass
                time.sleep(.1)
        self.default_window_handle = self.driver.current_window_handle
        if self.was_used:
            self.session_id = self.info_dict['session_id']
            with self.lock:
                self.logger.info('Session was used before')
        else:
            self.session_id = update_bot_sessions_start_sql(
                pool=self.db_conn,
                bot_id=self.id,
                site=self.website_settings[self.site]['site_id'],
                proxy=self.proxy,
                ref_id=self.ref_id,
                locale=f'{self.active_language}_{self.active_country}',
                ext_ip=self.active_ext_ip,
                country=self.active_country,
                language=self.active_language,
                user_agent=self.active_ua,
                alexa=1 if self.install_alexa_toolbar else 0,
                mobile=1 if self.is_mobile else 0,
                banner=self.clicked_ad
            )
        self.driver.maximize_window()
        if isinstance(self.driver, Firefox):
            self.session_created = True
        else:
            time.sleep(8)

    def save_session(self):
        cookies = self.driver.get_cookies()
        json_cookies = json.dumps(cookies)
        update_bot_sessions_finish_sql(
            pool=self.db_conn,
            cookies=json_cookies,
            clicked_banner=self.clicked_ad,
            last_inserted_id=self.session_id
        )

    def handle_bitcoin_talk_ref(self):
        site_and_locator = sec.choice(get_referrer_links_sql(
            pool=self.db_conn,
            website=self.site,
            ref_type="bitcointalk.org"
        ))
        site = site_and_locator[0]
        locator = site_and_locator[1]
        alt_locator = site_and_locator[2]
        with self.lock:
            self.logger.info(f"GET {site}")
        self.driver.get(site)
        if is_xpath_locator(locator):
            elem = self.driver.find_element_by_xpath(locator)
        else:
            elements = self.driver.find_elements_by_css_selector(locator)
            if not elements:
                elements = self.driver.find_elements_by_css_selector(alt_locator)
            elem = sec.choice(elements)
        elem.click()

    def handle_google_ref(self):
        site_and_exit = sec.choice(
            get_referrer_links_sql(
                pool=self.db_conn,
                website=self.site,
                ref_type="google.com"
            )
        )
        site = site_and_exit[0]
        locator = site_and_exit[1]
        alt_locator = site_and_exit[2]
        with self.lock:
            self.logger.info(f"GET {site_and_exit[0]}")
        try:
            self.driver.get(site)
        except WebDriverException as we:
            if "about:neterror?" in str(we):
                time.sleep(sec.randint(2, 5))
                self.driver.refresh()
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if ("Error 404" or "Problem loading page") in self.driver.title:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if ("502" or "Gateway") in self.driver.title:
            time.sleep(7)
            self.driver.refresh()
        if "sorry" in self.driver.current_url:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if "captcha" in self.driver.find_element_by_tag_name("body").text:
            self.handle_twitter_ref()
        elements = None
        self.delete_target_attributes()
        if is_xpath_locator(locator):
            elem = self.driver.find_element_by_xpath(locator)
        else:
            elements = self.driver.find_elements_by_css_selector(locator)
            if not elements:
                elements = self.driver.find_element_by_css_selector(alt_locator)
            elem = sec.choice(elements)
        try:
            elem.click()
        except ElementNotInteractableException:
            elements.remove(elem)
            elem = sec.choice(elements)
            elem.click()
        except WebDriverException as we:
            if "about:neterror?" in str(we):
                time.sleep(sec.randint(2000, 5000) / 1000)
                self.driver.refresh()
        if ("Error 404" or "Problem loading page") in self.driver.title:
            time.sleep(sec.randint(100, 5000) / 1000)
            self.driver.refresh()
        if ("Error 502" or "Gateway") in self.driver.title:
            time.sleep(sec.randint(100, 5000) / 1000)
            self.driver.refresh()

    def handle_twitter_ref(self):
        try:
            # Get site to fetch and locator leading to next site
            site_and_loc = sec.choice(
                get_referrer_links_sql(
                    pool=self.db_conn,
                    website=self.site,
                    ref_type="twitter.com"
                )
            )
            locator = site_and_loc[1]

            # Browser request to site
            with self.lock:
                self.logger.info(f"GET {site_and_loc[0]}")
            self.driver.get(site_and_loc[0])

            # Wait 40 secs until all elements of type locator are located, then proceed
            WebDriverWait(self.driver, 33).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, locator)))
            time.sleep(sec.randint(1, 3))

            # Check for shitty random appearing overlay
            elements = self.driver.find_elements_by_xpath(
                "/html/body/div/div/div/div[1]/div[1]/div/div/div/div[2]/div[2]/div/div[2]/div[1]"
            )
            if elements:
                with self.lock:
                    self.logger.warning("Found shitty popup on twitter")
                elem = elements[0]
                elem.click()
            time.sleep(sec.randint(1, 3))
            self.delete_target_attributes()
            if is_xpath_locator(locator):
                elem = self.driver.find_element_by_xpath(locator)
            else:
                elem = self.driver.find_element_by_css_selector(locator)
            with self.lock:
                self.logger.info(f"Clicking on reflink: {elem.text}")
            elem.click()
            time.sleep(sec.randint(5, 20))
            if "t.co" in self.driver.current_url:
                with self.lock:
                    self.logger.info(f"Site loading error, repeating request")

                time.sleep(sec.randint(2, 5))
                self.driver.get(self.driver.current_url)
        except WebDriverException as we:
            if "about:neterror?" in str(we):
                time.sleep(sec.randint(2, 5))
                self.driver.refresh()

    def handle_onsite(self):
        while True:
            if self.ip_address_has_changed():
                return None
            time.sleep(sec.randint(1, 8))
            self.scroll_window()
            loc = sec.choice(
                get_website_locators_sql(
                    pool=self.db_conn,
                    website=self.site
                )
            )
            if is_xpath_locator(loc):
                elem = self.driver.find_element_by_xpath(loc)
            else:
                elem = self.driver.find_element_by_css_selector(loc)
            with self.lock:
                self.logger.info(f"Clicking on site element: {elem.text}")
            elem.click()

            time.sleep(sec.randint(5, 40))
            self.per_impression_chance_to_click_banner()
            if self.click_ad_allowed and self.clicks_ad:
                self.click_rnd_banner_or_not()

    def delete_target_attributes(self):
        self.driver.execute_script(
            "var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('target');}"
        )

    def delete_onclick_attribute(self):
        self.driver.execute_script(
            "var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('onclick');}"
        )

    def delete_rel_attribute(self):
        self.driver.execute_script(
            "var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('rel');}"
        )

    def scroll_window(self):
        try:
            percentage_from_screen = sec.randint(15, 80)
            self.driver.execute_script(
                f"var h=document.body.scrollHeight;window.scrollTo(0,h*{percentage_from_screen}/100);"
            )
            time.sleep(sec.randint(0, 3333) / 1000)
            self.driver.execute_script("window.scrollTo(0,0);")
        except JavascriptException as je:
            pass

    def ip_address_has_changed(self):
        _proxy = {'http': f'socks5://{self.proxy_ip}:{self.proxy_port}',
                  'https': f'socks5://{self.proxy_ip}:{self.proxy_port}'}
        ext_ip = None
        while ext_ip is None:
            try:
                ext_ip = requests.get('https://coinminingpool.org/api/ip', proxies=_proxy).text[1:]
            except Exception:
                time.sleep(sec.randint(1111, 2111) / 1000)
                pass
        if (ext_ip != self.active_ext_ip) and (ext_ip.count('.') == 3):
            with self.lock:
                self.logger.critical("IP Address has CHANGED")
            return True
        return False

    def get_ip_address_from_cmp(self):
        _proxy = {'http': f'socks5://{self.proxy_ip}:{self.proxy_port}',
                  'https': f'socks5://{self.proxy_ip}:{self.proxy_port}'}
        ext_ip = None
        while ext_ip is None:
            try:
                ext_ip = requests.get('https://coinminingpool.org/api/ip', proxies=_proxy).text[1:]
            except Exception:
                time.sleep(sec.randint(1111, 2111) / 1000)
                pass
        return ext_ip

    def per_impression_chance_to_click_banner(self):
        data = self.website_settings[self.site]['percent_ctr']
        if (500 - data * 100 / 2) < sec.randint(0, 1000) < (500 + data * 100 / 2):
            self.clicks_ad = True
        with self.lock:
            self.logger.info(f"Clicks ad banner: {self.clicks_ad}")

    def click_rnd_banner_or_not(self):
        possibles = self.find_possible_banners()
        if isinstance(possibles, list):
            banner = sec.choice(possibles)
            with self.lock:
                self.logger.warning(f"CLICKING ON BANNER!")
            if self.ip_address_has_changed():
                return None
            while self.clicked_ad == 0:
                try:
                    banner.click()
                    self.clicked_ad = 1
                    self.clicks_ad = False
                except ElementNotInteractableException:
                    possibles.remove(banner)
                    banner = sec.choice(possibles)
                    time.sleep(sec.randint(2000, 3333) / 1000)
            open_windows = self.driver.window_handles
            if len(open_windows) > 1:
                open_windows.remove(self.default_window_handle)
                for win in open_windows:
                    self.driver.switch_to.window(win)
                    time.sleep(sec.randint(2000, 3333) / 1000)
                    self.scroll_window()
                    try:
                        self.driver.find_element_by_tag_name('body').click()
                    except:
                        pass
                    time.sleep(sec.randint(2000, 13333) / 1000)
                    self.driver.close()
            self.driver.switch_to.window(self.default_window_handle)

    def find_possible_banners(self):
        possibles = []
        banner_0 = self.banner_is_present_728x90_coinzilla()
        if banner_0:
            possibles.append(banner_0)
            possibles.append(banner_0)
        banner_1 = self.banner_is_present_sticky_footer()
        if banner_1:
            possibles.append(banner_1)
        banner_2 = self.banner_is_present_widget()
        if banner_2:
            possibles.append(banner_2)
            possibles.append(banner_2)
        banner_3 = self.banner_is_present_alert()
        if banner_3:
            possibles.append(banner_3)
        if len(possibles) == 0:
            with self.lock:
                self.logger.info("No banners found")
            return False
        with self.lock:
            self.logger.info(f"Possible Banners: {len(possibles)}")
        return possibles

    def banner_is_present_alert(self):
        """Checks if alert banner is displayed and returns the iframe object if true"""
        self.scroll_window()
        banner = self.driver.find_elements_by_id('coinzilla_popup_wrapper')
        if not banner:
            return False
        links_in_banner = banner[0].find_elements_by_tag_name('a')
        if not links_in_banner:
            return False
        for _link in links_in_banner:
            href = _link.get_attribute('href')
            if 'request-global.czilladx.com/serve/click' not in href:
                links_in_banner.remove(_link)
        return sec.choice(links_in_banner)

    def banner_is_present_widget(self):
        """Checks if widget banner is displayed and returns the iframe object if true"""
        banner = self.driver.find_elements_by_class_name('coinzilla_widget_img_wrapper_link')
        if not banner:
            return False
        href = banner[0].get_attribute('href')
        if 'request-global.czilladx' not in href:
            return False
        return banner[0]

    def banner_is_present_sticky_footer(self):
        """Checks if sticky footer banner is displayed and returns the iframe object if true"""
        banner = self.driver.find_elements_by_id("zone-2915cbd4f18eefa9351")
        if not banner:
            return False
        self.driver.switch_to.frame(banner[0])
        inner = self.driver.find_elements_by_tag_name("a")
        if not inner:
            return False
        href = inner[0].get_attribute("href")
        if "request-global.czilladx" not in href:
            return False
        self.driver.switch_to.default_content()
        return banner[0]

    def banner_is_present_728x90_coinzilla(self):
        """Checks if 728x90 banner is displayed and returns the iframe object if true"""
        banner = self.driver.find_elements_by_id("Z-5185cbd4f18e967f55")
        if not banner:
            return False
        self.driver.switch_to.frame(banner[0])
        inner = self.driver.find_elements_by_tag_name("a")
        if not inner:
            return False
        href = inner[0].get_attribute("href")
        if "marketplace" in href:
            return False
        if "request-global.czilladx" not in href:
            return False
        self.driver.switch_to.default_content()
        return banner[0]

    def _banner_is_present_728x90_cointraffic(self):
        """
        Checks if widget banner is displayed and returns the iframe object if true
        """
        banner = self.driver.find_elements_by_css_selector("span[id*='ct_*_disp']")
        if not banner:
            return False
        links_in_banner = banner[0].find_elements_by_tag_name('a')
        if not links_in_banner:
            return False
        for _link in links_in_banner:
            width = _link.get_attribute('width')
            if 'request-global.czilladx.com/serve/click' not in width:
                links_in_banner.remove(_link)

    def get_info_from_proxy_ip(self) -> dict:
        _proxy = {'http': f'socks5://{self.proxy_ip}:{self.proxy_port}',
                  'https': f'socks5://{self.proxy_ip}:{self.proxy_port}'}
        resp = None
        api_key_0 = "c35452a139fb4a8b89d7d0c10c02533f"
        api_key_1 = "5fbe0d543a4346fb8b0f22dbe03a05df"
        api_key_2 = "4e1064584c174efabc2df9ecc3696ff1"
        api_key_3 = "ccd5aa8a662448d1954e47ac1e5ebd81"
        api_key = sec.choice([api_key_0, api_key_1, api_key_2, api_key_3])
        while not resp:
            try:
                resp = json.loads(requests.get(
                    url="http://api.ipgeolocation.io/ipgeo",
                    params={"apiKey": api_key},
                    proxies=_proxy).content
                )
                ext_ip = resp["ip"]
                if not ext_ip:
                    ext_ip = requests.get('https://coinminingpool.org/api/ip', proxies=_proxy).text[1:]
                    time.sleep(5)
                country = resp["country_code2"]
                if not country:
                    country = sec.choice(["RU", "US"])
                lang_list = resp['languages']
                if "," not in lang_list:
                    lang = lang_list
                else:
                    lang = sec.choice(lang_list.split(","))
                if not lang:
                    lang = "en"

                g_domain = f"https://google{resp['country_tld']}"
                if not g_domain:
                    g_domain = f"https://google.com"

                return {
                    'external_ip': ext_ip,
                    'country': country,
                    'language': lang,
                    'google_domain': g_domain,
                }
            except InsecureRequestWarning:
                pass

            except Exception:
                time.sleep(2)
                pass

    def close_and_quit(self):
        with self.lock:
            self.logger.warning(f'Closing!')
        time.sleep(sec.randint(0, 3333) / 1000)
        if isinstance(self.driver, Firefox) and (len(self.driver.window_handles) > 1):
            self.driver.close()
        if isinstance(self.driver, Firefox):
            self.driver.quit()


class TrafficBotInstance(Process):

    def __init__(self, _id, pipe_end, _logger, stdout_lock):

        Process.__init__(self)
        self.name = f"Instance{self.id}"
        self.id = _id
        self.dpp_id = f'P{self.id}'
        self.com_chan_parent = pipe_end
        self.com_chan_child = None
        self.logger = _logger.getChild(self.name)
        self.logger.setLevel(logging.DEBUG)
        self.lock = stdout_lock
        self.database_connection = None
        self.last_time_alive = None
        self.instance = None
        self.com_queue = []

    def run(self) -> None:
        with self.lock:
            self.logger.info('Starting TrafficBotInstance')
            self.logger.debug('Creating database connection...')

        # Create MariaDB connection pool
        self.database_connection = create_connection_pool(str(int(self.id) + 1))

        # Start main loop in TrafficBotInstance
        while True:
            self.lock.acquire()
            self.logger.info('Starting new main loop iteration.')
            self.lock.release()
            # Reset instance and timestamp-log on loop beginning
            self.instance = None
            self.last_time_alive = None

            self.lock.acquire()
            self.logger.debug("Create multiprocessing pipe for bot initialization.")
            self.lock.release()

            self.com_chan_child, com_chan = Pipe()
            # Create new TrafficBotProcess and start it
            self.instance = TrafficBotProcess(
                bid=self.id,
                database_connection=self.database_connection,
                communication_channel=com_chan,
                lock_obj=self.lock,
                log=self.logger
            )

            if not self.instance:
                self.lock.acquire()
                self.logger.debug("Failed spawning TrafficBotProcess, retrying after timeout.")
                self.lock.release()
                time.sleep(25)
                continue

            while True:
                if not self.instance.is_alive():
                    self.instance.start()
                    self.last_time_alive = time.time()
                    self.com_chan_parent.send(DPP(self.dpp_id, '0', 'LOG', time.time()))

                # Check for full pipes
                if self.com_chan_child.poll():
                    self.com_queue.append(self.com_chan_child.recv())
                if self.com_chan_parent.poll():
                    self.com_queue.append(self.com_chan_parent.recv())

                # Check job queue
                if self.com_queue:
                    unlocked = self.handle_communication_packages()
                    if unlocked:
                        break
                time.sleep(.61)

    def handle_timeouts(self):
        pass

    def handle_communication_packages(self):
        while len(self.com_queue) > 0:
            # Pop job from com_queue
            dpp = self.com_queue.pop(0)

            # Forward if necessary
            if dpp.TO != self.dpp_id:
                self.lock.acquire()
                self.logger.debug(f"Forwarding pipe packages")
                self.lock.release()
                if (dpp.FROM == '0') and (dpp.TYPE == 'PXY'):
                    self.com_chan_child.send(dpp)
                elif (dpp.FROM[0] == 'B') and (dpp.TYPE == 'PXY'):
                    self.com_chan_parent.send(dpp)
                else:
                    self.logger.debug(f"Unknown transmitter in Package")

            elif (dpp.TO == self.dpp_id) and (dpp.TYPE == "LOG"):
                self.last_time_alive = dpp.DATA if dpp.DATA else time.time()
            elif (dpp.TO == self.dpp_id) and (dpp.TYPE == "END"):
                return True
            else:
                self.logger.debug(f"Unknown TYPE in Package")
        return False


class TrafficBotInstanceManager:
    """
    Top Level TrafficBot class.

    For synchronization and communication between TrafficBotProcessManager (tb-manager)
     and daemon processes, called TrafficBotInstances (tb-instance).
     Synchronisation and communication is realized through duplex
     communication channels (pipes) and a multiprocessing Lock object
     instantiated in tb-manager, that is initialized in every tb-instance,
     to sync logging to stdout.



    """

    def __init__(self, instances_count):
        self._stdout_lock = Lock()
        self.dpp_id = '0'
        self.start_time = None
        self.logger = logging.getLogger('TrafficManager')
        self.logger.setLevel(logging.DEBUG)
        self.ic = instances_count
        self.instances = {f'{r}': None for r in range(1, self.ic + 1)}
        self.com_chan = {f'{r}': Pipe() for r in range(1, self.ic + 1)}
        self.monitoring = {f'{r}': {'ts': None, "pxy": None} for r in range(1, self.ic + 1)}
        self.com_q = []
        self.proxy_list = PROXIES

    def _send_proxy(self, bot_id):
        proxy = self.proxy_list.pop(0)
        self.monitoring[bot_id]["ts"] = time.time()
        self.monitoring[bot_id]["pxy"] = proxy
        self.com_chan[bot_id].send(DPP('0', f'B{bot_id}', 'PXY', proxy))

    def _add_proxy(self, proxy):
        self.proxy_list.append(proxy)

    def _start_instance(self, x):
        self.instances[str(x)] = TrafficBotInstance(
            _id=x,
            _logger=self.logger,
            pipe_end=self.com_chan[str(x)][1],
            stdout_lock=self._stdout_lock
        )
        self.com_chan[str(x)] = self.com_chan[str(x)][0]
        self.monitoring[str(x)]["ts"] = time.time()
        self.instances[str(x)].start()

    def handle_communication_packages(self):
        while len(self.com_q) > 0:
            # Get job
            dpp = self.com_q.pop(0)

            # Forward if necessary
            if dpp.TO != self.dpp_id:
                self.logger.debug("Forwarding pipe output to receiver")
                self.com_chan[dpp.TO[1]].send(dpp)

            # Process addressed to own address
            if self.dpp_id == dpp.TO:
                if dpp.TYPE == "LOG":
                    if isinstance(dpp.DATA, float):
                        self.monitoring[dpp.FROM[-1]]["ts"] = dpp.DATA
                    elif isinstance(dpp.DATA, str):
                        self.monitoring[dpp.FROM[-1]]["pxy"] = dpp.DATA
                elif dpp.TYPE == "PXY":
                    if not dpp.DATA:
                        self._send_proxy(dpp.FROM[-1])
                    elif isinstance(dpp.DATA, str):
                        self._add_proxy(dpp.DATA)
                else:
                    self.logger.debug("Unknown TYPE in Package...")

    def handle_timeouts(self):
        for x in self.monitoring:
            if self.monitoring[x]['ts'] and (time.time() > (self.monitoring[x]['ts'] + 450)):
                self.logger.debug("Found unresponsive process...")
                if self.instances[x].is_alive():
                    self.instances[x].terminate()
                self.com_chan[x], com_chan = Pipe()
                self.instances[x] = TrafficBotInstance(
                    _id=x,
                    _logger=self.logger,
                    pipe_end=com_chan,
                    stdout_lock=self._stdout_lock
                )
                self.instances[x].start()
                self.logger.debug("Rebooted the process...")

    def run(self):
        self.start_time = time.time()
        self.logger.debug("STARTING BOT!")
        # Start all instances
        for x in range(1, self.ic + 1):
            self._start_instance(x)
            time.sleep(2.5)

        while True:
            # Listen for incoming packages
            for i in self.com_chan:
                if self.com_chan[i].poll():
                    self.com_q.append(self.com_chan[i].recv())

            # Process packages
            if self.com_q:
                self.handle_communication_packages()

            self.handle_timeouts()

            time.sleep(1)


if __name__ == '__main__':
    try:
        manager = TrafficBotInstanceManager(4)
        manager.run()
    except KeyboardInterrupt:
        pass
    finally:
        sys.exit()
