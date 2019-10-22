import os
import sys
import time
import json
import pickle
import logging
import requests
import coloredlogs
import threading
import multiprocessing as mp
from socks import ProxyError
from secrets import SystemRandom
from pyvirtualdisplay import Display
from selenium.webdriver import Firefox
from libtrafficbot.blocked import BLOCKED
from user_agent import generate_user_agent
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxProfile
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (WebDriverException, ElementNotInteractableException,
                                        JavascriptException, NoSuchElementException, ElementClickInterceptedException)
from urllib3.exceptions import InsecureRequestWarning

from libtrafficbot.bot_helper import calculate_chance_weight_based, choose_fair, is_xpath_locator
from libtrafficbot.sql import database_connection_pool as db_pool
from libtrafficbot.sql.sql_funcs import (get_website_settings_sql, update_bot_sessions_finish_sql,
                                         update_bot_sessions_start_sql, get_referrer_links_sql,
                                         get_website_locators_sql, proxy_ext_ip_was_used, get_geoip_info_sql)

sec = SystemRandom()

LOGGER.setLevel(logging.WARNING)
CWD = os.getcwd()

logger = logging.getLogger('TRAFFICBOT')

u_log = logging.getLogger('urllib3')
u_log.setLevel(logging.CRITICAL)

s_log = logging.getLogger('socks')
s_log.setLevel(logging.WARNING)

e_log = logging.getLogger('easyprocess')
e_log.setLevel(logging.WARNING)

p_log = logging.getLogger('pyvirtualdisplay')
p_log.setLevel(logging.CRITICAL)

coloredlogs.install(
    level=logging.DEBUG,
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

proxy_queue = mp.Manager().Queue()


class TrafficBot(mp.Process):

    def __init__(self, bid: int, database_connection, proxy, log, lock_obj, headless=False):
        mp.Process.__init__(self)
        self.id = bid
        self.name = 'Bot'
        self.headless_firefox = headless
        self.logger = log.getChild(self.name)
        self.lock = lock_obj
        self.db_conn = database_connection
        # Randomly choose site for this Process
        self.website_settings = get_website_settings_sql(self.db_conn)
        self.site = calculate_chance_weight_based(
            [(s, self.website_settings[s]["global_site_weight"]) for s in self.website_settings]
        )
        self.clicks_ad = False
        self.clicked_ad = 0
        self.click_ad_allowed = self.website_settings[self.site]["ad_clicks_enabled"]

        self.proxy = proxy
        self.proxy_ip = None
        self.proxy_port = None
        self.active_ext_ip = None
        self.was_used = None
        self.info_dict = None
        self.active_country = None
        self.active_language = None
        self.active_google_domain = None
        self.active_ua = None
        self.is_mobile = None
        self.error_counter = None
        self.rotation_time = None
        self.install_alexa_toolbar = None
        self.ref_id, self.ref = None, None
        self.session_id = None
        self.driver = None
        self.first_inited = None
        self.session_created = None
        self.default_window_handle = None
        self.screen_x = None
        self.screen_y = None
        self.pickled_profile = None
        self.xvfb = None

    def create_display(self):
        try:
            # Emulating headless window manager to run Firefox in non headless mode
            size = sec.choice(
                [(1920, 1080) for _ in range(100)] +
                [(1280, 720) for _ in range(20)] +
                [(4096, 2160) for _ in range(20)] if (self.is_mobile is False) else []
            )
            return Display(visible=False, size=(size[0], size[1]), use_xauth=True).start()
        except:
            pass
        with self.lock:
            self.logger.warning("Failed instantiating virtual display manager. Fallback to headless Firefox")
        self.headless_firefox = True
        os.environ['MOZ_HEADLESS'] = '1'

        return None

    def run(self) -> None:
        self.error_counter = 0
        try:
            while True:
                if self.error_counter > 3:
                    raise Exception('Error during process initialization!')

                self.first_init()
                if not self.first_inited:
                    self.error_counter += 1
                    continue

                self.create_session()
                if not self.session_created:
                    self.error_counter += 1
                    continue

                if self.first_inited and self.session_created:
                    break

            WebDriverWait(self.driver, sec.randint(500, 3000) / 1000)
            self.ref()

            WebDriverWait(self.driver, sec.randint(500, 8000) / 1000)
            self.handle_onsite()

            WebDriverWait(self.driver, sec.randint(500, 8000) / 1000)
            self.save_session()
            self.close_and_quit()

            with self.lock:
                self.logger.debug('Session saved to database...')
        except ProxyError as pe:
            if not self.ip_address_has_changed():
                with self.lock:
                    self.logger.error("ProxyError" + pe.msg)

    def first_init(self):
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
                self.logger.warning(f"Same external IP used in last 10 minutes: {self.active_ext_ip}")
            self.logger.debug(f"Fetched IP geolocation and meta data via {'database.' if self.was_used else 'HTTP request.'}")
            self.logger.debug(f"Socks5 proxy URL   : {self.proxy}")
            self.logger.debug(f"External IP address: {self.active_ext_ip}")
            self.logger.debug(f"Language code      : {self.active_language}")
            self.logger.debug(f"Country code       : {self.active_country}")
            self.logger.debug(f"Google domain      ; {self.active_google_domain}")
            self.logger.debug(f"User agent         : {self.active_ua}")
            self.logger.debug(f"Is mobile device   : {self.is_mobile}")
            self.logger.info(f'Session website    : {self.site}')
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

        self.pickled_profile = pickle.dumps(profile)

        self.xvfb = self.create_display()

        self.driver = Firefox(
            executable_path=f"{CWD}/firefox/driver/geckodriver",
            firefox_binary=f"{CWD}/firefox/binary/firefox-bin",
            firefox_profile=profile,
        )
        self.driver.set_page_load_timeout(125)
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

    def save_session(self):
        try:
            if self.driver:
                cookies = self.driver.get_cookies()
                json_cookies = json.dumps(cookies)
                update_bot_sessions_finish_sql(
                    pool=self.db_conn,
                    cookies=json_cookies,
                    clicked_banner=self.clicked_ad,
                    last_inserted_id=self.session_id
                )
        except:
            pass

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
        with self.lock:
            self.logger.info(f"GET {site_and_exit[0]}")
        try:
            self.driver.get(site)
        except WebDriverException as we:
            if self.ip_address_has_changed():
                return False
            WebDriverWait(self.driver, sec.randint(500, 3000) / 1000)
            if ("about:neterror?" or "document.body is null") in str(we):
                self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
            else:
                self.handle_twitter_ref()

        if self.ip_address_has_changed():
            return False

        if ("Error 404" or "Problem loading page") in self.driver.title:
            WebDriverWait(self.driver, sec.randint(2000, 5000) / 1000)
            try:
                self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
            except Exception as e:
                self.logger.error(e)

        if ("502" or "Gateway") in self.driver.title:
            WebDriverWait(self.driver, sec.randint(2000, 5000) / 1000)
            self.driver.refresh()

        if "sorry" in self.driver.current_url:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))

        if "captcha" in self.driver.find_element_by_tag_name("body").text:
            self.handle_twitter_ref()

        elem = None
        self.delete_target_attributes()
        if is_xpath_locator(locator):
            elem = self.driver.find_element_by_xpath(locator)
        else:
            elements = self.driver.find_elements_by_css_selector(locator)
            if not elements:
                self.handle_twitter_ref()
            try:
                elem = sec.choice(elements)
            except IndexError:
                self.handle_twitter_ref()

        try:
            elem.click()
        except AttributeError:
            self.run()
        except WebDriverException as we:
            if self.ip_address_has_changed():
                return False
            if "about:neterror?" in str(we):
                WebDriverWait(self.driver, sec.randint(2000, 5000) / 1000)
                self.driver.refresh()
        if self.ip_address_has_changed():
            return False
        if ("Error 404" or "Problem loading page") in self.driver.title:
            WebDriverWait(self.driver, sec.randint(2000, 5000) / 1000)
            self.driver.refresh()
        if ("Error 502" or "Gateway") in self.driver.title:
            WebDriverWait(self.driver, sec.randint(2000, 5000) / 1000)
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
            WebDriverWait(self.driver, sec.randint(500, 5000) / 1000)

            # Check for shitty random appearing overlay
            elements = self.driver.find_elements_by_xpath(
                "/html/body/div/div/div/div[1]/div[1]/div/div/div/div[2]/div[2]/div/div[2]/div[1]"
            )
            if elements:
                with self.lock:
                    self.logger.warning("Found shitty popup on twitter")
                elem = elements[0]
                elem.click()
            WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
            self.delete_target_attributes()
            if is_xpath_locator(locator):
                elem = self.driver.find_element_by_xpath(locator)
            else:
                elem = self.driver.find_element_by_css_selector(locator)
            with self.lock:
                self.logger.info(f"Clicking on reflink: {elem.text}")
            elem.click()
            WebDriverWait(self.driver, sec.randint(5000, 20000) / 1000)
            if "t.co" in self.driver.current_url:
                with self.lock:
                    self.logger.info(f"Site loading error, repeating request")

                WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
                self.driver.refresh()
        except WebDriverException as we:
            if self.ip_address_has_changed():
                return False
            if "about:neterror?" in str(we):
                WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
                self.driver.refresh()

    def handle_onsite(self):
        """Routine for handling one of the main websites"""
        while True:

            if self.ip_address_has_changed():
                return False
            time.sleep(sec.randint(1, 8))
            self.scroll_window()
            loc = sec.choice(
                get_website_locators_sql(
                    pool=self.db_conn,
                    website=self.site
                )
            )
            elem = None
            if is_xpath_locator(loc):
                try:
                    elem = self.driver.find_element_by_xpath(loc)
                except NoSuchElementException:
                    pass
            else:
                try:
                    elem = self.driver.find_element_by_css_selector(loc)
                except NoSuchElementException:
                    elem = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, loc)))
            if not isinstance(elem, WebElement):
                return False
            with self.lock:
                self.logger.info(f"Clicking on site element: {elem.text}")
            try:
                elem.click()
            except ElementClickInterceptedException:
                self.run()
            except WebDriverException:
                self.close_and_quit()
            WebDriverWait(self.driver, sec.randint(1200, 55000) / 1000)
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

    def scroll_window(self, coordinates: tuple = None):
        try:
            if not coordinates:
                percentage_from_screen = sec.randint(15, 80)
                self.driver.execute_script(
                    f"var h=document.body.scrollHeight;window.scrollTo(0,h*{percentage_from_screen}/100);"
                )
                time.sleep(sec.randint(0, 3333) / 1000)
                self.driver.execute_script("window.scrollTo(0,0);")
            else:
                x = str(coordinates[0])
                y = str(coordinates[1])
                self.driver.execute_script(
                    f"var h=document.body.scrollHeight;window.scrollTo({x},{y});"
                )
        except JavascriptException as jse:
            with self.lock:
                self.logger.error(jse)

    def ip_address_has_changed(self):
        _proxy = {
            'http': f'socks5://{self.proxy_ip}:{self.proxy_port}',
            'https': f'socks5://{self.proxy_ip}:{self.proxy_port}'
        }
        ext_ip = None
        while ext_ip is None:
            try:
                ext_ip = requests.get(
                    url='https://coinminingpool.org/api/ip',
                    proxies=_proxy
                ).text[1:]
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
                    WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
            open_windows = self.driver.window_handles
            if len(open_windows) > 1:
                open_windows.remove(self.default_window_handle)
                for win in open_windows:
                    self.driver.switch_to.window(win)
                    WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
                    self.scroll_window()
                    try:
                        self.driver.find_element_by_tag_name('body').click()
                    except:
                        pass
                    WebDriverWait(self.driver, sec.randint(1000, 5000) / 1000)
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
        banner = None
        try:
            banner = self.driver.find_elements_by_id("Z-5185cbd4f18e967f55")
        except Exception as e:
            if "not clickable at point" in str(e):
                x = str(e).split("point (")[1].split(",")[0]
                y = str(e).split(") because")[0].split(",")[-1]
                self.scroll_window(coordinates=(x, y))
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
        if isinstance(self.driver, Firefox) and (len(self.driver.window_handles) > 1):
            self.driver.close()
        if isinstance(self.driver, Firefox):
            self.driver.quit()
        if isinstance(self.xvfb, Display):
            self.xvfb.stop()


class TrafficBotMonitorDaemon(threading.Thread):
    """
    Daemon thread to manage TrafficBotProcesses
    """

    def __init__(self, d_id, d_logger, stdout_lock, db_conn, user_stop_event=None):

        threading.Thread.__init__(self)
        self.id = d_id
        self.name = f"Daemon{self.id}"
        self.daemon = True
        self.logger = d_logger.getChild(self.name)
        self.logger.setLevel(logging.DEBUG)
        self.lock = stdout_lock
        self.database_connection = db_conn
        self.last_time_alive = time.time()
        self.proc_first_time_alive = None
        self.stop_daemon_event = threading.Event()
        self.child_process = None
        self.active_proxy = None
        self.user_stop_event = user_stop_event or threading.Event()

    def run(self) -> None:
        with self.lock:
            self.logger.debug(f'Starting Daemon {self.id}')

        # Start main loop in TrafficBotInstance
        while True:

            # If is None start
            if not self.child_process:
                self.start_process()

            # If process is dead check for KeyboardInterrupt or normal finish
            elif isinstance(self.child_process, mp.Process) and (not self.child_process.is_alive()):
                if self.stop_daemon_event.is_set():
                    break
                self.add_to_proxy_queue(self.active_proxy)
                self.start_process()

            # if alive check for possible timeout errors
            elif isinstance(self.child_process, mp.Process) and self.child_process.is_alive():
                if time.time() > self.proc_first_time_alive + 420:
                    self.child_process.terminate()
                    with self.lock:
                        self.logger.critical("Found unresponsive Process...")
                    self.stop_daemon_event.set()

            if time.time() > self.last_time_alive + 60:
                self.last_time_alive = time.time()

            if self.stop_daemon_event.is_set():
                break

            time.sleep(.25)

    def start_process(self):
        with self.lock:
            self.logger.debug('Creating new TrafficBot process.')

        # Get proxy from queue
        proxy = self.get_proxy_from_queue()

        # Create new TrafficBotProcess and start it
        self.child_process = TrafficBot(
            bid=self.id,
            proxy=proxy,
            database_connection=self.database_connection,
            lock_obj=self.lock,
            log=self.logger
        )
        self.child_process.start()

        # Log timestamp
        self.proc_first_time_alive = time.time()

    def get_proxy_from_queue(self):
        self.active_proxy = proxy_queue.get()
        with self.lock:
            self.logger.debug(f"Fetched from proxy_list: {self.active_proxy}")
        return self.active_proxy

    def add_to_proxy_queue(self, proxy):
        proxy_queue.put(proxy)
        with self.lock:
            self.logger.debug(f"Added proxy to proxy_list: {proxy}")

    def kill_daemon(self):
        self.child_process.terminate()
        self.stop_daemon_event.set()
        self.logger.debug("TrafficBotInstance Stopped")
        return True


class TrafficBotInstanceManager:
    """
    Top Level TrafficBot class.

    For synchronization and communication between TrafficBotProcessManager (tb-manager)
     and daemon threads, called TrafficBotInstances (tb-instance).
     Synchronisation and communication is realized through duplex
     communication channels (pipes) and a multiprocessing Lock object
     instantiated in tb-manager, that is initialized in every tb-instance,
     to sync logging to stdout.

    Parameter:
        instances_count - Number of daemon processes to spawn
    """

    def __init__(self, instances_count):
        self.ic = instances_count
        self.logger = logging.getLogger('TrafficManager')
        self.logger.setLevel(logging.DEBUG)
        self.lock = mp.Lock()
        self.start_time = time.time()
        self.daemons = {f'{r}': None for r in range(1, self.ic + 1)}
        self.keep_alive_timestamps = {f'{r}': None for r in range(1, self.ic + 1)}

        self.proxy_blacklist = []
        for PXY in PROXIES:
            proxy_queue.put(PXY)

        self.database_connection = db_pool(str(10))

    def start_daemon(self, x):
        if isinstance(x, int):
            x = str(x)
        self.daemons[x] = TrafficBotMonitorDaemon(
            d_id=x,
            d_logger=self.logger,
            stdout_lock=self.lock,
            db_conn=self.database_connection
        )
        self.daemons[x].start()

    def handle_daemon_timeouts(self):
        for x in self.daemons.keys():
            if self.daemons[str(x)].last_time_alive and (time.time() > (self.daemons[str(x)].last_time_alive + 180)):
                with self.lock:
                    self.logger.critical(f"Found unresponsive daemon thread #{x}...")
                time.sleep(5)
                self.start_daemon(str(x))
                with self.lock:
                    self.logger.debug("Rebooted the daemon thread...")
            if not self.daemons[str(x)].is_alive():
                self.start_daemon(str(x))
                with self.lock:
                    self.logger.debug("Rebooted the daemon thread...")

    def run(self):
        ts = time.time()
        self.logger.debug("STARTING TRAFFIC MANAGER!")

        while True:

            for d in self.daemons.keys():
                if not self.daemons[d]:
                    self.start_daemon(d)

                elif isinstance(self.daemons[d], threading.Thread):
                    if not self.daemons[d].is_alive():
                        with self.lock:
                            self.logger.critical(f"Found dead daemon thread #{d}...restarting")
                        self.start_daemon(d)
                    elif self.daemons[d].is_alive() and (time.time() > self.daemons[d].last_time_alive + 120):
                        with self.lock:
                            self.logger.critical(f"Found unresponsive daemon thread #{d}...restarting")
                        self.start_daemon(d)

            # Send Keep-Alive debug message every minute
            if time.time() > ts + 60:
                with self.lock:
                    self.logger.info("Manager is still alive")
                ts += 60
            time.sleep(.1)

    def quit(self):
        for r in self.daemons:
            self.daemons[r].kill_daemon()
        self.logger.debug("KILLED BOT by user!")


if __name__ == '__main__':
    PROCESSES = 5
    manager = TrafficBotInstanceManager(PROCESSES)
    try:
        manager.run()
    except KeyboardInterrupt:
        manager.quit()
    finally:
        sys.exit()
