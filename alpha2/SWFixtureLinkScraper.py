import logging
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

logging.basicConfig(level = logging.INFO)

class SWFixtureLinkScraper:
    '''
    This class handles the process of scraping links to the fixture pages on soccerway.
    '''

    def __init__(self, link, league, season):
        self._link = link
        self._season = season
        self._league = league


    def getBrowser(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        browser = webdriver.Chrome(options=chrome_options, executable_path="../chromedriver.exe")
        browser.get(self._link)
        WebDriverWait(browser, 10).until(expected_conditions.presence_of_element_located((
            By.XPATH, r'//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]'
        )))
        browser.maximize_window()
        gdpr_button = browser.find_element_by_xpath(
            r'//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]')  # the button is located by xPath
        browser.execute_script("arguments[0].click();", gdpr_button)  # executes JavaScript to click button
        return browser

    def traverse(self):
        fixture_links = []

        self._browser = self.getBrowser()
        table_list = self._browser.find_element_by_id("page_competition_1_block_competition_matches_summary_11_page_dropdown")
        for option in table_list.find_elements_by_tag_name('option'):
            time.sleep(3.5)
            result_table = self.findResultsTable()
            soup = BeautifulSoup(result_table.get_attribute('innerHTML'), 'lxml')

            for match in soup.findAll('td', {'class' : 'score-time'}):
                href = match.a.get('href')
                fixture_links.append("https://uk.soccerway.com/" + href)

            option.click()

        return {(self._league, self._season): fixture_links}

    def findResultsTable(self):
        WebDriverWait(self._browser, 25).until(expected_conditions.presence_of_element_located((
            By.XPATH, r'//*[@id="page_competition_1_block_competition_matches_summary_11"]/div[3]/table/tbody')))
        return self._browser.find_element_by_xpath(
            r'//*[@id="page_competition_1_block_competition_matches_summary_11"]/div[3]/table/tbody')


def main(request):
    # TIMER START
    start = time.time()

    scraper = SWFixtureLinkScraper('https://uk.soccerway.com/national/germany/bundesliga/20202021/', 'D1', '20202021')
    print(scraper.traverse())

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
