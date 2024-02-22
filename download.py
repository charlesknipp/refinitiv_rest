from refinitiv_rest import *
from time import sleep

from multiprocessing import Process,Queue
from tracker import Tracker
import queue

import pandas as pd

## SERIAL DOWNLOADER ##########################################################

def chunks(array,n):
    m = len(array)
    for i in range(0,m,n):
        if i+n > m:
            # reached end of array
            yield [array[i],array[-1]]
        else:
            # return bounds of chunked array
            yield [array[i],array[i+n-1]]

class Downloader:
    out_of_space = False
    def __init__(self,extraction,start_date,end_date=None,line_num=1):
        self.start_date = start_date
        self.end_date = end_date
        self.task = extraction

        self.session  = extraction.session
        self.base_ric = extraction.security.base_ric

        interval_end  = start_date if end_date is None else end_date
        date_interval = "%s-%s" % (self.start_date,interval_end)
        self.filename = "%s.csv.gz" % (date_interval)

        report_type  = "(%s)" % self.task.report_type
        task_info    = "%s %s %s" % (self.base_ric,report_type,date_interval)
        self.tracker = Tracker(line_num,task_info)

        # make request
        self.make_request()
        self.download_request()

    def make_request(self):
        self.request = self.task.request(self.start_date,self.end_date)
        self.tracker.begin_tracking("Requesting")

        # progress check
        self.check_request()

    def make_request(self):
        try:
            self.tracker.begin_tracking("Requesting")
            self.request = self.task.request(self.start_date,self.end_date)
        except Exception:
            # sometimes the actual request fails because of too many API calls
            self.task = self.unmodified_extraction
            sleep(10)
            self.make_request()

        json_response = self.session.check_status(self.request,timeout=60)        
        if json_response.status_code == 200:
            self.tracker.end_tracking("Requested")
            sleep(2)
        else:
            self.tracker.end_tracking("Request Failed")

            # exception handling occurs in the API interface
            self.task = self.unmodified_extraction
            self.make_request()

    def download_request(self):
        # this has been acting up recently
        self.tracker.begin_tracking("Downloading")
        
        try:
            # download completed request and split into daily files
            self.task.download_report(self.filename)
            self.task.split_files(self.filename)
            self.tracker.end_tracking("Downloaded")
        except OSError as e:
            if e.errno == 28:
                self.out_of_space = True
                self.tracker.end_tracking("Insufficient Disk Space")
            else:
                self.tracker.end_tracking("Download Failed")
                self.download_request()
        except Exception as e:
            self.tracker.end_tracking("Download Failed")
            self.download_request()
                

## TEST CASES #################################################################

def download_queue(task_list,proc_id):
    while True:
        try:
            task = task_list.get_nowait()
        except queue.Empty:
            break
        else:
            Downloader(*task,proc_id)

def parallel_download(security,start_date,end_date,num_procs=8,num_dates=2):
    date_range = pd.date_range(start_date,end_date).strftime("%Y-%m-%d").to_list()
    task_list  = Queue()

    for date_chunk in chunks(date_range,num_dates):
        task_list.put([security,*date_chunk])

    processes = []
    for i in range(num_procs):
        proc = Process(
            target = download_queue,
            args = (task_list,i+1),
            name = "Download-%d" % (i)
        )

        processes.append(proc)
        proc.start()
    
    for proc in processes:
        proc.join()

def serial_download(security,start_date,end_date):
    Downloader(security,start_date,end_date)

## MAIN #######################################################################

if __name__ == "__main__":
    ## create a new session
    auth = Session(USERNAME,PASSWORD)

    ## for historical searches, we can do the following:
    historical_search(auth,"ES","2017-12-04","2017-12-06")

    ## download multiple dates in parallel requests
    parallel_download(
        EndOfDay(auth,Futures("ES","US/Central")),
        start_date = "2023-01-01",
        end_date = "2024-01-01",
        num_procs = 20,
        num_dates = 3
    )