import json
import logging
import os
import sys
from json import JSONDecodeError
from multiprocessing import Process, Queue
from time import sleep, time

import requests
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import coloredlogs
from selenium.common.exceptions import WebDriverException, ElementNotInteractableException
from selenium.webdriver import FirefoxProfile
from secrets import SystemRandom

from selenium.webdriver import Firefox
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.exceptions import MaxRetryError
from user_agent import generate_user_agent

from blocked import BLOCKED

sec = SystemRandom()


def data_dict():
    return {
        "coinminingpool.org": {
            'refs':
                {
                    'btctalk': [
                        ("https://bitcointalk.org/index.php?topic=4779906.0",
                         "a[href*='coinminingpool.org']"),
                        ("https://bitcointalk.org/index.php?topic=1809920.msg43996527#msg43996527",
                         "a[href*='coinminingpool.org']"),
                        ("https://bitcointalk.org/index.php?topic=5036256.0",
                         "a[href*='coinminingpool.org']"),
                        ("https://bitcointalk.org/index.php?topic=5036256.msg46084686#msg46084686",
                         "a[href*='coinminingpool.org']"),
                        ("https://bitcointalk.org/index.php?action=profile;threads;u=1192292;sa=showPosts",
                         "a[href*='coinminingpool.org']"),
                    ],
                    'twitter': [
                        ('https://twitter.com/coinminingpool',
                         "a[href*='eO9TjqE8iC']"),
                        ('https://twitter.com/DCx7C5',
                         "a[href*='eO9TjqE8iC']")
                    ],
                    'google': [
                        (f'https://www.google.com/search?q=coinminingpool.org',
                         "a[href*='coinminingpool.org']"),
                        (f'https://www.google.com/search?q=coin mining pool org&num=100',
                         "a[href*='coinminingpool.org']"),
                        (f'https://www.google.com/search?q=coin mining pool&num=100',
                         "a[href*='coinminingpool.org']"),
                    ]
                },
            'onsite_elements': [
                '//*[@id="mainNavbar"]/li[1]',
                '//*[@id="mainNavbar"]/li[2]',
                '//*[@id="mainNavbar"]/li[3]',
                '//*[@id="mainNavbar"]/li[4]',
                '//*[@id="maintable1"]/tbody/tr[1]/td[2]',
                '//*[@id="maintable1"]/tbody/tr[2]/td[2]',
                '//*[@id="maintable1"]/tbody/tr[3]/td[2]',
                '//*[@id="maintable1"]/tbody/tr[4]/td[2]',
                '//*[@id="maintable1"]/tbody/tr[5]/td[2]']
        },
        "cryptogiveaways.de": {
            'refs':
                {
                    'twitter': [
                        ('https://twitter.com/coin__giveaway',
                         "a[href*='6uXc5pGKny']"),
                    ],
                    #'google': [
                    #    (f'https://www.google.com/search?q=cryptogiveaways.de&num=500',
                    #     "a[href*='cryptogiveaways.de']"),
                    #]
                },
            'onsite_elements': [
                '/html/body/nav/div/div/ul/li[1]/a',
                '/html/body/nav/div/div/ul/li[2]/a',
                '/html/body/nav/div/div/ul/li[3]/a',
                '/html/body/nav/div/div/ul/li[4]/a',
                '/html/body/nav/div/div/ul/li[0]/a']
        },
    }


def is_xpath_locator(locator_string: str) -> bool:
    """
    Checks if locator string is XPath Element
    """
    if locator_string.startswith("/"):
        return True
    return False


class TrafficBot:

    def __init__(self, proc_queue, job, headless=False):
        self.pq = proc_queue
        self.job = job
        self.id = self.job[0]
        # Randomly choose site for this Process
        self.site = sec.choice(
            [
                "coinminingpool.org",
                "cryptogiveaways.de",
            ]
        )

        self.clicks_ad = False
        # Runs traffic bot without actually open Firefox Browser
        if headless is True:
            os.environ['MOZ_HEADLESS'] = '1'

        # Load Proxy
        self.proxy = self.job[1]
        self.proxy_ip = self.proxy.split('//')[1].split(':')[0]
        self.proxy_port = int(self.proxy.split('//')[1].split(':')[1])

        # Get geo ip info
        self.info_dict = self.get_info_from_proxy_ip()
        self.active_ext_ip = self.info_dict['external_ip']
        self.active_country = self.info_dict['country']
        self.active_language = self.info_dict['language']
        self.active_google_domain = self.info_dict['google_domain']

        # 14% chance traffic is mobile
        if 430 < sec.randint(0, 1000) < 570:
            self.active_ua = generate_user_agent(device_type='smartphone')
            self.is_mobile = True
        else:
            self.active_ua = generate_user_agent(device_type='desktop')
            self.is_mobile = False

        self.profile = FirefoxProfile('firefox/profile/user0')
        self.profile.set_preference(
            'intl.accept_languages', '{}en-US;q=0.9,en;q=0.8'.format(
                f'{self.active_language}_{self.active_country},' if self.active_language is not None else ""
            ))

        self.profile.set_preference('general.useragent.override', self.active_ua)
        self.profile.set_preference('network.proxy.http', f'{self.proxy_ip}')
        self.profile.set_preference('network.proxy.http_port', self.proxy_port)
        self.profile.set_preference('network.proxy.ssl', f'{self.proxy_ip}')
        self.profile.set_preference('network.proxy.ssl_port', self.proxy_port)
        self.profile.set_preference('network.proxy.type', 1)
        self.profile.set_preference('network.proxy.no_proxies_on', ",".join(BLOCKED))
        self.profile.update_preferences()

        # Create error counter
        self.error_counter = 0

        # Create anti bounce counter
        self.anti_bounce_counter = 0

        # Proxy rotation time management
        self.site_data = data_dict()
        self.logger = logging.getLogger(__name__)
        coloredlogs.install(
            level=logging.INFO,
            fmt=f'%(asctime)-20s- %(processName)-5s - %(levelname)-7s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.rotation_time = 300
        self.reference_time = 1569783600
        self.next_proxy_change_time = None
        self.set_next_proxy_change_time()
        self.click_stream = []
        self.logger.info(f"Worker Id: {self.id}")
        self.logger.info(f"Website: {self.site}")
        self.logger.info(f"Proxy: {self.proxy}")
        self.logger.info(f"External IP: {self.active_ext_ip}")
        self.logger.info(f"Country: {self.active_country}")
        self.logger.info(f"Google Domain: {self.active_google_domain}")
        self.logger.info(f"Language: {self.active_language}")
        self.logger.info(f"User-Agent (Full rnd): {self.active_ua}")
        self.logger.info(f"Is smartphone (Chance: 14%): {self.is_mobile}")
        self.logger.info(f"Clicks ad banner (Chance: 0.5%): {self.clicks_ad}")
        self.logger.info(f"Click Stream: {str(self.click_stream)}")

        # Create browser session
        self.driver = Firefox(
            executable_path="firefox/driver/geckodriver",
            firefox_binary="firefox/binary/firefox",
            firefox_profile=self.profile,
        )
        # Delete all cookies, etc
        self.driver.delete_all_cookies()

        # Install Alexa sidebar plugin
        if 410 < sec.randint(0, 1000) < 680:
            self.logger.info("Installing Alexa toolbar plugin!")
            self.driver.install_addon("/home/daen/trafficbot/firefox/extensions/alxf-4.0.0.xpi")

        self.ref = None

        ref = sec.choice([r for r in self.site_data[self.site]["refs"].keys()])
        if ref == "btctalk":
            self.ref = self.handle_bitcoin_talk_ref
        elif ref == "twitter":
            self.ref = self.handle_twitter_ref
        elif ref == "google":
            self.ref = self.handle_google_ref

    def run(self) -> None:
        try:
            self.ref()
            sleep(sec.randint(2, 10))
            self.handle_onsite()
        except KeyboardInterrupt:
            self.logger.error(f'Closing! User interaction')
        except MaxRetryError:
            self.logger.error(f'Closing! Too many errors!')
        except JSONDecodeError:
            self.logger.error(f'Closing! JSonDecodeError')
        finally:
            self.logger.warning(f'Closing!')
            self.close()
            sleep(sec.randint(0, 3333) / 1000)
            self.pq.put((self.id, self.proxy))

    def handle_bitcoin_talk_ref(self):
        site_and_locator = sec.choice(self.site_data[self.site]["refs"]["btctalk"])
        site = site_and_locator[0]
        locator = site_and_locator[1]
        self.logger.info(f"GET {site}")
        self.driver.get(site)
        if is_xpath_locator(locator):
            elem = self.driver.find_element_by_xpath(locator)
        else:
            elements = self.driver.find_elements_by_css_selector(locator)
            elem = sec.choice(elements)
        elem.click()

    def handle_google_ref(self):
        site_and_exit = sec.choice(self.site_data[self.site]["refs"]["google"])
        site = site_and_exit[0]
        locator = site_and_exit[1]
        self.logger.info(f"GET {site_and_exit[0]}")
        try:
            self.driver.get(site)
        except WebDriverException:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if ("Error 404" or "Problem loading page") in self.driver.title:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if "sorry" in self.driver.current_url:
            self.driver.get(site.replace(self.active_google_domain, "https://google.com"))
        if "captcha" in self.driver.find_element_by_tag_name("body").text:
            self.handle_twitter_ref()
        elements = None
        if is_xpath_locator(locator):
            elem = self.driver.find_element_by_xpath(locator)
        else:
            elements = self.driver.find_elements_by_css_selector(locator)
            elem = sec.choice(elements)
        try:
            elem.click()
        except ElementNotInteractableException:
            elements.remove(elem)
            elem = sec.choice(elements)
            elem.click()

        if ("Error 404" or "Problem loading page") in self.driver.title:
            self.driver.refresh()

    def handle_twitter_ref(self):
        try:
            # Get site to fetch and locator leading to next site
            site_and_loc = sec.choice(self.site_data[self.site]["refs"]["twitter"])
            locator = site_and_loc[1]

            # Browser request to site
            self.logger.info(f"GET {site_and_loc[0]}")
            self.driver.get(site_and_loc[0])

            # Wait 40 secs until all elements of type locator are located, then proceed
            WebDriverWait(self.driver, 33).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, locator)))
            sleep(sec.randint(1, 3))

            # Check for shitty random appearing overlay
            elements = self.driver.find_elements_by_xpath("/html/body/div/div/div/div[1]/div[1]/div/div/div/div[2]/div[2]/div/div[2]/div[1]")
            if elements:
                self.logger.warning("Found shitty popup on twitter")
                elem = elements[0]
                elem.click()
            sleep(sec.randint(1, 3))
            self.delete_target_attributes()
            if is_xpath_locator(locator):
                elem = self.driver.find_element_by_xpath(locator)
            else:
                elem = self.driver.find_element_by_css_selector(locator)
            self.logger.info(f"Clicking on reflink: {elem.text}")
            elem.click()
            sleep(sec.randint(5, 20))
            if "t.co" in self.driver.current_url:
                self.logger.info(f"Site loading error, repeating request")
                sleep(sec.randint(2, 5))
                self.driver.get(self.driver.current_url)
        except WebDriverException as we:
            if "about:neterror?" in str(we):
                sleep(sec.randint(2, 5))
                self.handle_twitter_ref()

    def handle_onsite(self):
        while True:
            try:
                if self.ip_address_has_changed():
                    return None
                self.scroll_window()
                rest = self.next_proxy_change_time - time()
                self.logger.info(f"Time until proxy change: {rest}")
                if (self.anti_bounce_counter >= 4) and (rest > 80):
                    self.handle_google_ref()
                sleep(sec.randint(2, 10))
                loc = sec.choice(self.site_data[self.site]["onsite_elements"])
                if is_xpath_locator(loc):
                    elem = self.driver.find_element_by_xpath(loc)
                else:
                    elem = self.driver.find_element_by_css_selector(loc)
                self.logger.info(f"Clicking on site element: {elem.text}")
                elem.click()
                if self.ip_address_has_changed():
                    return None
                sleep(sec.randint(18, 28))
                self.per_impression_chance_to_click_banner()
                if "//coinminingpool.org" in self.driver.current_url:
                    possibles = self.find_possible_banners()
                    if self.clicks_ad and isinstance(possibles, list):
                        banner = sec.choice(possibles)
                        self.logger.info(f"CLICKING ON BANNER!")
                        try:
                            banner.click()
                        except ElementNotInteractableException:
                            possibles.remove(banner)
                            banner = sec.choice (possibles)
                            banner.click()

            except WebDriverException:
                sleep(11)

    def delete_target_attributes(self):
        self.logger.info("Deleting target attributes via JS execution")
        self.driver.execute_script("var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('target');}")

    def delete_onclick_attribute(self):
        self.logger.info("Deleting onclick attributes via JS execution")
        self.driver.execute_script("var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('onclick');}")

    def delete_rel_attribute(self):
        self.logger.info("Deleting rel attributes via JS execution")
        self.driver.execute_script("var c=document.getElementsByTagName('a');for(var i=0;i<c.length;i++){c[i].removeAttribute('rel');}")

    def scroll_window(self):
        self.logger.info("Scrolling window via JS execution")
        percentage_from_screen = sec.randint(15, 80)
        self.driver.execute_script(f"var h=document.body.scrollHeight;window.scrollTo(0,h*{percentage_from_screen}/100);")
        sleep(sec.randint(0, 3333) / 1000)
        self.driver.execute_script("window.scrollTo(0,0);")

    def set_next_proxy_change_time(self):
        t = self.reference_time
        while t < time():
            t += self.rotation_time
        if t < time():
            t += 300
        self.next_proxy_change_time = t

    def ip_address_has_changed(self):
        if time() > self.next_proxy_change_time:
            if time() > self.next_proxy_change_time - 10:
                sleep(sec.randint(5, 22))
            self.logger.critical("IP Address has CHANGED")
            return True
        return False

    def per_impression_chance_to_click_banner(self):
        # 1% chance Bot clicks on ad banner
        if 470 < sec.randint(0, 1000) < 530:
            self.clicks_ad = True
        else:
            self.clicks_ad = False
        self.logger.info(f"Clicks ad banner (Chance: 0.5%): {self.clicks_ad}")

    def find_possible_banners(self):
        possibles = []
        cwh = self.driver.current_window_handle
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
            self.logger.info("No banners found")
            return False
        self.logger.info(f"Possible Banners: {len(possibles)}")
        return possibles

    def banner_is_present_alert(self):
        """
        Checks if alert banner is displayed and returns the iframe object if true
        """
        self.scroll_window()
        banner = self.driver.find_elements_by_id('coinzilla_popup_wrapper')
        if not banner:
            return False
        self.logger.info("Banner located on site: Alert Native coinzilla.io")
        links_in_banner = banner[0].find_elements_by_tag_name('a')
        if not links_in_banner:
            return False
        for _link in links_in_banner:
            href = _link.get_attribute('href')
            if 'request-global.czilladx.com/serve/click' not in href:
                links_in_banner.remove(_link)
        return sec.choice(links_in_banner)

    def banner_is_present_widget(self):
        """
        Checks if widget banner is displayed and returns the iframe object if true
        """
        banner = self.driver.find_elements_by_class_name('coinzilla_widget_img_wrapper_link')
        if not banner:
            return False
        self.logger.info("Banner located on site: Widget Native coinzilla.io")
        href = banner[0].get_attribute('href')
        if 'request-global.czilladx' not in href:
            return False
        return banner[0]

    def banner_is_present_sticky_footer(self):
        """
        Checks if sticky footer banner is displayed and returns the iframe object if true
        """
        banner = self.driver.find_elements_by_id("zone-2915cbd4f18eefa9351")
        if not banner:
            return False
        self.logger.info("Banner located on site: Sticky-Footer coinzilla.io")
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
        """
        Checks if 728x90 banner is displayed and returns the iframe object if true
        """
        banner = self.driver.find_elements_by_id("Z-5185cbd4f18e967f55")
        if not banner:
            return False
        self.logger.info("Banner located on site: 728x90 coinzilla.io")
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
        self.logger.info("Banner located on site: 728x90 cointraffic.io")
        links_in_banner = banner[0].find_elements_by_tag_name('a')
        if not links_in_banner:
            return False
        for _link in links_in_banner:
            width = _link.get_attribute('width')
            if 'request-global.czilladx.com/serve/click' not in width:
                links_in_banner.remove(_link)

    def get_info_from_proxy_ip(self) -> dict:
        _proxy = {'http': f'http://{self.proxy_ip}:{self.proxy_port}',
                  'https': f'https://{self.proxy_ip}:{self.proxy_port}'}
        resp = None
        api_key_0 = "c35452a139fb4a8b89d7d0c10c02533f"
        api_key_1 = "c6d49ec452f24f59adb58a8c7ea59935"
        while not resp:
            try:
                resp = json.loads(requests.get(
                    url="http://api.ipgeolocation.io/ipgeo",
                    params={"apiKey": api_key_0 if (self.id < 5) else api_key_1},
                    proxies=_proxy).content
                )
                ext_ip = resp["ip"]
                if not ext_ip:
                    sleep(5)
                    continue
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
                    'google_domain': g_domain
                }
            except Exception as e:
                sleep(sec.randint(5, 12))
                continue

    def close(self):
        self.driver.close()
        self.driver.quit()
        del self.driver


class TrafficBotProcess(TrafficBot, Process):

    def __init__(self, *args, **kwargs):
        Process.__init__(self)
        TrafficBot.__init__(self, *args, **kwargs)


def standard_multiprocessing():
    pq = Queue()
    job_list = [
        (2, 'https://190.2.153.131:38240'),
        (5, 'https://190.2.153.131:38243'),
        (10, 'https://190.2.153.131:38304'),
        (9, 'https://190.2.153.131:38303'),
        (7, 'https://190.2.153.131:38301'),
        (4, 'https://190.2.153.131:38242'),
        (1, 'https://190.2.153.131:38239'),
        (8, 'https://190.2.153.131:38302'),
        (3, 'https://190.2.153.131:38241'),
        (6, 'https://190.2.153.131:38244')]
    sec.shuffle(job_list)
    for i in job_list:
        pq.put(i)
    try:
        while True:
            p0 = TrafficBotProcess(pq, pq.get(), True)
            p1 = TrafficBotProcess(pq, pq.get(), True)
            p2 = TrafficBotProcess(pq, pq.get(), True)
            p3 = TrafficBotProcess(pq, pq.get(), True)
            #p4 = TrafficBotProcess(pq, pq.get(), True)

            p0.start()
            sleep(2)
            p1.start()
            sleep(2)
            p2.start()
            sleep(2)
            p3.start()
            sleep(2)
            #p4.start()
            #sleep(2)

            p0.join()
            p1.join()
            p2.join()
            p3.join()
            #p4.join()

    except KeyboardInterrupt:
        sys.exit()


if __name__ == '__main__':
    standard_multiprocessing()
    """    try:
            pc = sys.argv[1]
        except IndexError:
            pc = 5
        try:
            headless = sys.argv[2]
        except IndexError:
            headless = True
        try:
            tbm = TrafficBotManager(
                proc_count=pc,
                headless=headless
            )
            tbm.start_bots()
        except KeyboardInterrupt:
            sleep(1)
        finally:
            sys.exit()"""

