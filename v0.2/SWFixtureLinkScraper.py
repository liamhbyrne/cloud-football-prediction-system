import logging
import time

from selenium import webdriver

logging.basicConfig(level = logging.INFO)

class SWFixtureLinkScraper:
    '''
    This class handles the process of scraping links to the fixture pages on soccerway.
    '''

    def __init__(self, link):
        self._browser = self.getBrowser()
        if self._browser is not None:
            self._browser.get(link)

    def getBrowser(self):
        chrome_options = webdriver.ChromeOptions()
        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        return webdriver.Chrome(options=chrome_options, executable_path="../chromedriver.exe")


def main(request):
    # TIMER START
    start = time.time()

    scraper = SWFixtureLinkScraper('https://uk.soccerway.com/national/germany/bundesliga/20212022/')

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
