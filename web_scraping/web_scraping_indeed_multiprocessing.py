import math
# importing the multiprocessing module
import multiprocessing
from time import sleep
from selenium import webdriver
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import random


# define scraping function
def scrape_indeed(search, loc, limit=50, canada=False):
    # search_term is the keyword/designation to be searched
    search_term = search.replace(' ', '+')

    if canada:
        url = 'https://www.indeed.ca/jobs?q={}&l={}&limit={}&radius=25&start=0'.format(search_term, loc, limit)
    else:
        url = 'https://www.indeed.com/jobs?q={}&l={}&limit={}&radius=25&start=0'.format(search_term, loc, limit)

    # Start the browser and load the above URL
    browser = webdriver.Chrome('/Users/viethnguyen/Documents/Projects/NLP_with_Python/chrome_driver/chromedriver')
    browser.get(url)

    # Empty dataframe in which we will store our data scraped from job posts
    data = pd.DataFrame(columns=['job_title', 'company', 'location', 'job_description'])

    x = 0

    # get the number of results. This determines
    num_results = browser.find_element_by_id('searchCountPages').text
    ind0 = num_results.find('of ') + 3
    ind1 = num_results.find(' ', ind0)
    num_results = int(num_results[ind0:ind1])
    pages = math.ceil(num_results / limit)  # the number of pages to visit.

    # Loop through the pages
    for j in range(pages):

        # All the job posts have class 'row result clickcard'.
        job_elements = browser.find_elements_by_xpath(
            "//div[@class='jobsearch-SerpJobCard unifiedRow row result clickcard']")

        # Loop through the individual job posts
        for i in range(len(job_elements)):

            # Click on the job post
            job_elements[i].click()

            # Sleep for minimum 3 seconds because we dont want to create unnecessary load on Indeed's servers
            sleep(3 + random.randint(0, 3))

            # Sometimes Selenium might start scraping before the page finishes loading or
            # we might encounter '404 : Job not found error'
            # Although these occurences are very rare we don't want our job scrapper to crash.
            # Therefore we will retry before moving on.
            # If the data was successfully scrapped then it will break out of the for loop
            # If we encounter error it will retry again provided the retry count is below 5

            done = False
            for k in range(0, 5):
                try:
                    title = browser.find_element_by_id('vjs-jobtitle').text
                    company = browser.find_element_by_id('vjs-cn').text
                    company = company.replace('- ', '')

                    location = browser.find_element_by_id('vjs-loc').text
                    description = browser.find_element_by_id('vjs-desc').text
                    done = True
                    break
                except NoSuchElementException:
                    print('Unable to fetch data. Retrying.....')

            if not done:
                continue

            # For debugging purposes lets log the job post scrapped
            print('Completed Post {} of Page {} - {}'.format(i + 1, j + 1, title))

            # Insert the data into our dataframe
            data = data.append({'job_title': title,
                                'company': company,
                                'location': location,
                                'job_description': description}, ignore_index=True)

            # Change the URL, so as to move on to the next page
        url = url.replace('start=' + str(x), 'start=' + str(x + limit))
        x += limit

        if len(job_elements) < limit:
            break

        browser.get(url)
        print('Moving on to page ' + str(j + 2))
        sleep(2)

        # A popover appears when we go to the next page. We will tell the browser to click on close button.
        # Although so far for me it has appeared only on 2nd page but I have included the check for every page to be on safer side
        try:
            browser.find_element_by_id('popover-x').click()
        except:
            print('No Newsletter Popup Found')

    browser.close()
    return data


# download data, use Toronto as an example
#loc = 'Toronto%2C+ON'
#q = 'title%3A%28machine+learning%29'

#df0 = scrape_indeed(q, loc, 50, True)  # Jan 25
#df0.to_pickle('data_scientist_toronto.pkl')

# multi thread class
class scrapeProcess (multiprocessing.Process):
   def __init__(self, name, search, loc, limit=50, canada=False):
      multiprocessing.Process.__init__(self)
      self.canada = canada
      self.limit = limit
      self.loc = loc
      self.search = search
      self.name = name
   def run(self):
      print("Starting {}, pid {}".format(self.name, self.pid))
      df0 = scrape_indeed(self.search, self.loc, self.limit, self.canada)
      df0.to_pickle('data_scientist_' + self.loc + '.pkl')
      print("Exiting {}, pid {}".format(self.name, self.pid))


# download data, with the following cities
cities = ['boston', 'chicago', 'la', 'ny', 'sf', 'montreal', 'toronto', 'vancouver']
loc_list = ['Boston%2C+MA', 'Chicago%2C+IL', 'Los Angeles%20+CA', 'New%20+York%2C+NY',
            'San%20+Francisco%2C+CA', 'Montreal%2C+QC', 'Toronto%2C+ON', 'Vancouver%2C+BC']
q = 'title%3A%28machine+learning%29'

process_list = []
for i in range(len(cities)):
    name = 'scrapeProcess {}'.format(i)
    if i > 4:
        process = scrapeProcess(name, q, loc_list[i], 50, True)  # Jan 25
    else:
        process = scrapeProcess(name, q, loc_list[i], 50, False)  # Jan 25
    process_list.append(process)

for item in process_list:
    item.start()

for item in process_list:
    item.join()

print("All processes complete")
for item in process_list:
    print("Process {}, pid {} is alive: {}".format(item.name, item.pid, item.is_alive()))