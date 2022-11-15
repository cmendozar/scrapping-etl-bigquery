import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_jobs(day: str) -> pd.DataFrame:#, user_id: str, 
               # pass_id:str, uri: str, db: str, collection: str) -> None:

    # REQUEST THE PAGE
    r = requests.get(url='https://www.getonbrd.com/empleos-data')
    html = r.text

    # EXTRACT JOBS FROM URL
    soup = BeautifulSoup(html, features="html.parser")
    raw_jobs = soup.find_all(attrs={"class": "remote"})

    # TRANSFORM DATA
    jobs = []
    for raw_job in raw_jobs:
        job_dict = {}
        job = BeautifulSoup(str(raw_job), features="html.parser")
        job_dict['name'] = job.find('strong').string
        attr = [attr for attr in job.find(
            attrs={'class': "size0"}).stripped_strings]
        job_dict['company'] = attr[0]
        try:
            job_dict['location'] = attr[1].split('\n')[0]
            job_dict['location_extra'] = str(attr[1].split('\n')[1])
        except:
            job_dict['location'] = str(attr[1])
            job_dict['location_extra'] = ''

        try:
            job_dict['level'] = job.find('span').string.split('|')[0]
            job_dict['work_day'] = job.find('span').string.split('|')[1]
        except:
            job_dict['level'] = job.find('span').string
            job_dict['work_day'] = ''

        job_dict['link'] = job.find('a').get('href')
        job_dict['portal'] = 'getonboard'
        job_dict['search_date'] = day #datetime.datetime.strptime(day, "%Y-%m-%d")
        job_dict['publish_date'] = job.find(
            attrs={'class': "gb-results-list__date color-hierarchy3"}).string.split('\n')[1]
        job_dict['key'] = job_dict['name'] + \
            job_dict['company'] + job_dict['publish_date']
        jobs.append(job_dict)

    jobs_dataframe = pd.DataFrame(jobs)

    print(f" Have been extracted and transformed {len(jobs_dataframe)} rows of information into DataFrame")
    return jobs_dataframe