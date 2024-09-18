from selenium import webdriver
import csv
import time
from reader import ReportReader

if __name__ == '__main__':
    file = open('listofcompanies.csv', 'r')
    csv_file = csv.reader(file)
    driver = webdriver.Chrome()
    # for line in csv_file:
    #     company_name = line[3]
    #     driver.get("https://google.com/search?q=" + company_name)
        
        # time.sleep(5)
    report_reader = ReportReader("Company.pdf")
    report_reader.read()
    print(report_reader.summary)