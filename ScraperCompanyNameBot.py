from selenium import webdriver
import time
from selenium.webdriver.common.by import By
import csv

if __name__ == "__main__":
    file = open("listofcompanies.csv", "r")
    csv_file = csv.reader(file)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--user-data-dir=Refinitiv")
    prefs = {
        "download.default_directory": r"/Users/christinelee/Desktop/Selgrad-RA/reports",
        "savefile.default_directory": r"/Users/christinelee/Desktop/Selgrad-RA/reports",
        "plugins.always_open_pdf_externally": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)

    driver.get(
        "https://workspace.refinitiv.com/web/layout?layoutTemplate=marketMonitoring"
    )
    time.sleep(25)

    successful_download = False

    def find_all_iframes(drive):
        global successful_download
        iframes = drive.find_elements(By.XPATH, "//iframe")
        for index, iframe in enumerate(iframes):
            drive.switch_to.frame(index)
            try:
                dropdown_button_xpath = "/html/body/gs-root/div[1]/gs-top-panel/div/coral-header/div[4]/gs-export-options/div/gs-dropdown-button[2]/div/coral-button[2]"
                dropdown_button = driver.find_element(By.XPATH, dropdown_button_xpath)
                print(dropdown_button)
                dropdown_button.click()
                print("Clicked the dropdown successfully!")
                time.sleep(12)
                download_detail_guidance_report_xpath = "/html/body/gs-root/div[1]/gs-top-panel/div/coral-header/div[4]/gs-export-options/div/gs-dropdown-button[2]/div/emerald-popup-menu/coral-item[2]"
                download_button = driver.find_element(
                    By.XPATH, download_detail_guidance_report_xpath
                )
                download_button.click()
                print("Downloaded report successfully!")
                successful_download = True
                time.sleep(6)
            except:
                find_all_iframes(drive)
                drive.switch_to.parent_frame()

    next(csv_file)
    for line in csv_file:
        company_name = line[3]
        company_tic = line[1]
        potential_urls = [
            f"https://workspace.refinitiv.com/web/Apps/Corp/?s={company_tic}&st=RIC#/Apps/GuidanceSummary",
            f"https://workspace.refinitiv.com/web/Apps/Corp/?s={company_tic}.O&st=RIC#/Apps/GuidanceSummary",
            f"https://workspace.refinitiv.com/web/Apps/Corp/?s={company_tic}.PK&st=RIC#/Apps/GuidanceSummary",
            f"https://workspace.refinitiv.com/web/Apps/Corp/?s={company_tic}.PQ&st=RIC#/Apps/GuidanceSummary",
        ]
        successful_download = False
        for company_url in potential_urls:
            driver.get(company_url)
            print("Going to page:", company_url)
            time.sleep(15)
            try:
                find_all_iframes(driver)
            except:
                if successful_download:
                    break
