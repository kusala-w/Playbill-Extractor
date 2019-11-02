import requests
import csv
import datetime
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.parse import parse_qs
from pymongo import MongoClient


main_website_url = 'http://www.playbill.com'
gross_page_url = main_website_url + '/grosses'
print("Started scraping " + gross_page_url + " at " + str(datetime.datetime.now()))

page_count = 0
#all_show_data = []
#all_show_data.append(["Show Id", "Show Name", "Week Ending", "Week Number", "This Week Gross", "Potential Gross", "Diff Dollars", "Avg Ticket", "Top Ticket", "Seats Sold", "Seats in Theatre", "Perfs", "Previews", "Cap This Week", "Diff cap"])

full_page = requests.get(gross_page_url)
full_page_soup = BeautifulSoup(full_page.text,'html.parser')
page_count += 1

main_show_table = full_page_soup.find("table", class_="bsp-table")

if main_show_table != None:
    table_body = main_show_table.find("tbody")

    if table_body != None:
        show_records = table_body.findAll("tr")

        db_client = MongoClient("mongodb+srv://application-user:1qaz2wsx!!@testcluster-kfiky.mongodb.net/test?retryWrites=true&w=majority")
        playbill_db = db_client.get_database("playbill_db")
        playbill_info_records = playbill_db.show_info
        
        for show in show_records:
            show_data = []
            show_id = ''
            show_link = show.find("a")
            show_name = show_link.contents[0].text
            show_url = urlparse(show_link.get("href"))
            query_parameters = parse_qs(show_url.query) #urlparse.pa(show_url.query)         
            if 'production' in query_parameters:
                show_id = query_parameters['production'][0]

            if show_id == '':
                # TODO: Log error that show id is not available in the query string. Hence continue with the next record.
                continue

            # Check if the Show Id is in the DB. If it is, do not go to the show page, just get the data for the current week from this record.
            show_count = playbill_info_records.count_documents({"show_id":show_id})
            if show_count > 0:
                #TODO: Get the data from the current record to insert in the DB
                print("Skipping show " + show_name)
                continue

            #If the show is not in DB, open the show page and scrape through all weeks.

            show_page_url = main_website_url + show_link.get("href")
            show_page = requests.get(show_page_url)
            show_soup = BeautifulSoup(show_page.text, 'html.parser')
            page_count += 1

            show_table = show_soup.find("table", class_="bsp-table")
            if show_table != None:
                show_table_body = show_table.find("tbody")
                if show_table_body != None:
                    weekly_data = show_table_body.findAll("tr")
                    if weekly_data != None:
                        for week in weekly_data:
                            week_ending = week.find("td",attrs={'data-label':'Week Ending'}).contents[1].text
                            week_number = week.find("td",attrs={'data-label':'Week Number'}).contents[1].text
                            gross = week.find("td",attrs={'data-label':'This Week Gross'})
                            this_week_gross = gross.find("span",class_="data-value").text
                            potential_gross = gross.find("span",class_="subtext").text
                            diff_dollars = week.find("td",attrs={'data-label':'Diff $'}).contents[1].text
                            avg_ticket_wrapper = week.find("td",attrs={'data-label':'Avg Ticket'})
                            avg_ticket = avg_ticket_wrapper.find("span",class_="data-value").text
                            top_ticket = avg_ticket_wrapper.find("span",class_="subtext").text
                            seats_wrapper = week.find("td",attrs={'data-label':'Seats Sold'})
                            seats_sold = seats_wrapper.find("span",class_="data-value").text
                            seats_in_theatre = seats_wrapper.find("span",class_="subtext").text
                            perf_wrapper = week.find("td",attrs={'data-label':'Perfs'})
                            perfs = perf_wrapper.find("span",class_='data-value').text
                            previews = perf_wrapper.find("span",class_='subtext').text
                            cap = week.find("td",attrs={'data-label':'% Cap This Week'}).contents[1].text
                            diff_cap = week.find("td",attrs={'data-label':'Diff % cap'}).contents[1].text

                            #all_show_data.append([show_id, show_name, week_ending,week_number,this_week_gross,potential_gross,diff_dollars,avg_ticket,top_ticket,seats_sold,seats_in_theatre,perfs,previews,cap,diff_cap])
                            show_week_data = {'show_id': show_id, 
                                            'show_name': show_name, 
                                            'week_ending': week_ending,
                                            'week_number': week_number,
                                            'this_week_gross': this_week_gross,
                                            'potential_gross': potential_gross,
                                            'diff_dollars': diff_dollars,
                                            'avg_ticket': avg_ticket,
                                            'top_ticket': top_ticket,
                                            'seats_sold': seats_sold,
                                            'seats_in_theatre': seats_in_theatre,
                                            'perfs': perfs,
                                            'previews': previews,
                                            'cap': cap,
                                            'diff_cap':diff_cap
                                            }
                            temp_val = 1

                            show_data.append(show_week_data)

            playbill_info_records.insert_many(show_data)
            print("Inserted data for show " + show_name)

        print("Finished scraping " + str(page_count) + " page(s) at " + str(datetime.datetime.now()))

        '''
        print("Started writing to CSV at " + str(datetime.datetime.now()))
        csvFileName = os.getcwd() + "\\Scrape\\playbill.csv"
        with open(csvFileName,'w',newline='') as output_file:
            output = csv.writer(output_file)
            for show_week in all_show_data:
                output.writerow(show_week)
        print("Finished writing to CSV at " + str(datetime.datetime.now()) + "\nThe output is available at " + csvFileName)
        '''