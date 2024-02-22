import requests,json
import os

import hashlib
import pandas as pd

## DEFINE GLOBAL VARIABLES ####################################################

BASE_URL = "https://selectapi.datascope.refinitiv.com/RestApi/v1/"

USERNAME = ""
PASSWORD = ""

PROXY = {
    "http": "",
    "https": ""
}

DATA_DIR = "data"

## SESSION CLASS ##############################################################

class Session:
    token = ""

    def __init__(self,username,password):
        self.username = username
        self.password = password

        # generate a token upon initialization
        self.authenticate()

    def authenticate(self):
        headers = {
            "Prefer": "respond-async",
            "Content-Type": "application/json; odata=minimalmetadata"
        }

        data = {
            "Credentials": {
                "Username": self.username,
                "Password": self.password,
            }
        }

        json_response = requests.post(
            BASE_URL + "Authentication/RequestToken",
            data = json.dumps(data),
            headers = headers,
            proxies = PROXY
        )

        response = json.loads(json_response.text)

        try:
            self.token = response["value"]
        except:
            message = "Authentication Error: " + json.dumps(response,indent=4)
            raise Exception(message)
        
    def check_authorization(self):
        headers = {
            "Authorization": "Token %s" % (self.token),
            "Prefer": "respond-async",
        }
        json_response = requests.get(
            BASE_URL + "Users/Users(%s)" % (self.username),
            headers = headers,
            proxies = PROXY
        )

        response = json.loads(json_response.text)
        return response

    def check_usage(self):
        headers = {
            "Authorization": "Token %s" % (self.token),
            "Prefer": "respond-async",
        }

        json_response = requests.get(
            BASE_URL + "Quota/GetQuotaInformation",
            headers = headers,
            proxies = PROXY
        )

        response = json.loads(json_response.text)
        return response
    
    def check_status_async(self,job_id):        
        request_header = {
            "Authorization": "Token %s" % (self.token),
            "Content-Type": "application/json",
            "Accept-Charset": "UTF-8",
            "Prefer": "respond-async",
        }

        # wait for the request to return a status code 200
        json_response = requests.get(
            BASE_URL + "Extractions/ExtractRawResult(ExtractionId='%s')" % (job_id),
            headers = request_header,
            proxies = PROXY
        )

        return json_response
    
    def check_status(self,job_id,timeout):
        # get requests timeout in 30s intervals
        max_counter = timeout // 30
        counter = 0

        status_code = 202
        json_response = None
        
        # if requests timeout according to our criteria, the API returns a 202
        while (status_code == 202):
            json_response = self.check_status_async(job_id)
            status_code = json_response.status_code
            
            if (counter >= max_counter):
                print("Exceeded maximum status checks")
                return 202
            
            counter += 1

        return json_response

## EXTRACTION CLASS ###########################################################

class Extraction:
    job_id = None
    odata_type = "#DataScope.Select.Api.Extractions.ExtractionRequests."
    report_type = ""

    def __init__(self,session,security):
        session.check_authorization()
        self.session  = session
        self.token    = session.token
        self.security = security

        self.condition = {
            "MessageTimeStampIn": "LocalExchangeTime",
            "DateRangeTimeZone": "UTC",
            "ReportDateRangeType": "Range"
        }

        self.identifiers = {
            "@odata.type": self.odata_type + "InstrumentIdentifierList",
            "InstrumentIdentifiers": [
                {
                    "Identifier": self.security.chain_rics,
                    "IdentifierType": self.security.ric_type,
                }
            ],
            "ValidationOptions": None,
            "UseUserPreferencesForValidationOptions": "false"
        }

    def get_valid_content(self,provider):
        headers = {
            "Authorization": "Token %s" % (self.token),
            "Content-Type": "application/json",
            "Accept-Charset": "UTF-8",
            "Prefer": "respond-async, wait=1",
        }

        request_type  = "DataScope.Select.Api.Extractions.ReportTemplates.ReportTemplateTypes'%s'" % (provider)
        json_response = requests.get(
            BASE_URL + "Extractions/GetValidContentFieldTypes(ReportTemplateType=%s)" % (request_type),
            headers = headers,
            proxies = PROXY
        )

        fieldnames = {}
        for item in json.loads(json_response.text)["value"]:
            try:
                fieldnames[item["FieldGroup"]].append(item["Name"])
            except:
                fieldnames[item["FieldGroup"]] = [item["Name"]]

        return fieldnames
    
    def request(self,start_date,end_date,fieldnames):
        end_date = start_date if end_date is None else end_date
        timezone = self.security.timezone

        # query based on calendar date, not trading date
        local_start_date = start_date + "T00:00:00.000000"
        local_end_date   = end_date + "T23:59:59.999999"
        
        utc_start_date = convert_to_utc(local_start_date,timezone)
        utc_end_date   = convert_to_utc(local_end_date,timezone)

        return self.localized_request(utc_start_date,utc_end_date,fieldnames)

    def localized_request(self,start_date,end_date,fieldnames):
        self.condition["QueryStartDate"] = start_date
        self.condition["QueryEndDate"]   = end_date

        headers = {
            "Content-Type": "application/json",
            "Prefer": "respond-async, wait=1",
            "Accept-Encoding": "gzip",
            "Authorization": "Token %s" % (self.token),
        }

        data = {
            "ExtractionRequest": {
                "@odata.type": self.odata_type,
                "ContentFieldNames": fieldnames,
                "IdentifierList": self.identifiers,
                "Condition": self.condition,
            }
        }

        json_response = requests.post(
                BASE_URL + "Extractions/ExtractRaw",
                headers = headers,
                data = json.dumps(data,sort_keys=True),
                proxies = PROXY
            )

        try:
            # successful job creation returns a location header
            result_url = json_response.headers["Location"]
            self.job_id = result_url.split("'")[1]
        except KeyError:
            # TODO: identify which field caused the job not to generate
            resp_notes = json.loads(json_response.text)["error"]["message"]
            err = requests.RequestException("Invalid Request: %s" % (resp_notes))
            raise err
        
        return self.job_id
    
    def get_output_filepath(self):
        directory = os.path.join(DATA_DIR,self.security.base_ric,self.report_type)
        if not os.path.exists(directory):
            os.makedirs(directory)

        return directory

    def download_report(self,filename):
        headers = {
            "Content-Type": "application/json",
            "Prefer": "respond-async, wait=1",
            "Accept-Encoding": "gzip",
            "X-Direct-Download": "true",
            "Authorization": "Token %s" % (self.token),
        }

        try:
            # download directly from AWS
            json_response = requests.get(
                BASE_URL + "Extractions/RawExtractionResults('%s')/$value" % (self.job_id),
                headers = headers,
                proxies = PROXY,
                stream = True
            )

            json_response.decode_content = False
            filepath = os.path.join(self.get_output_filepath(),filename)
            with open(filepath,"wb") as f:
                f.write(json_response.raw.read())

        except Exception as e:
            raise e
        
    def split_files(self,filename):
        start_date = filename[0:10]
        end_date   = filename[11:21]

        output_dir = self.get_output_filepath()
        old_filepath = os.path.join(output_dir,filename)

        # if the file is empty, just delete it
        if os.stat(old_filepath).st_size == 0:
            os.remove(old_filepath)

        if end_date == start_date:
            # rename files which only download a single date
            new_filename = "%s.csv.gz" % (start_date)
            os.rename(
                old_filepath,
                os.path.join(output_dir,new_filename)
            )
        else:
            # break up bulk data into daily files
            bulk_data = pd.read_csv(old_filepath)
            dates = pd.date_range(start_date,end_date).strftime("%Y-%m-%d").to_list()
            
            # EndOfDay data is treated differently
            date_col = "Trade Date" if self.report_type == "EndOfDay" else "Date-Time"
            filedates = pd.to_datetime(bulk_data[date_col])

            for date in dates:
                daily_data = bulk_data.loc[filedates == date]
                new_filename = "%s.csv.gz" % (date)
                daily_data.to_csv(
                    os.path.join(output_dir,new_filename),
                    compression = "gzip",
                    index = False
                )

            # once daily files are saved, delete the old file
            os.remove(old_filepath)
        
class HighFreq(Extraction):
    def __init__(self,session,security):
        Extraction.__init__(self,session,security)
        self.report_type = "HighFreq"

        # change condition values (applicable to all)
        self.condition["DisplaySourceRIC"] = "true"

    def request_trades(self,start_date,end_date=None,fieldnames=None):
        self.condition["ApplyCorrectionsAndCancellations"] = "true"
        self.odata_type += "TickHistoryTimeAndSalesExtractionRequest"
        if fieldnames is None:
            fieldnames = [
                "Trade - Price",
                "Trade - Volume",
                "Trade - Accumulated Volume",
                "Trade - Sequence Number",
                "Trade - Exchange Time"
            ]
        
        return Extraction.request(self,start_date,end_date,fieldnames)
    
    def request_quotes(self,start_date,end_date=None,fieldnames=None):
        self.condition["ApplyCorrectionsAndCancellations"] = "true"
        self.odata_type += "TickHistoryTimeAndSalesExtractionRequest"
        if fieldnames is None:
            fieldnames = [
                "Quote - Bid Price",
                "Quote - Bid Size",
                "Quote - Ask Price",
                "Quote - Ask Size",
                "Quote - Sequence Number",
                "Quote - Exchange Time"
            ]
        
        return Extraction.request(self,start_date,end_date,fieldnames)
    
    def request_depth(self,start_date,end_date=None,fieldnames=None):
        # for depth requests, change the condition settings
        self.condition["View"] = "NormalizedLL2"
        self.condition["NumberOfLevels"] = 10

        self.odata_type += "TickHistoryMarketDepthExtractionRequest"
        if fieldnames is None:
            fieldnames = [
                "Ask Price",
                "Ask Size",
                "Bid Price",
                "Bid Size",
                "Number of Buyers",
                "Number of Sellers"
            ]

        return Extraction.request(self,start_date,end_date,fieldnames)
    
    def request(self,start_date,end_date=None,fieldnames=None):
        match self.report_type:
            case "Trades":
                return self.request_trades(start_date,end_date,fieldnames)
            case "Quotes":
                return self.request_quotes(start_date,end_date,fieldnames)
            case "Depths":
                return self.request_depth(start_date,end_date,fieldnames)
            case _:
                raise Exception("data type not defined")
    
    def get_valid_content(self):
        time_and_sales = Extraction.get_valid_content(self,"TickHistoryTimeAndSales")
        market_depth = Extraction.get_valid_content(self,"TickHistoryMarketDepth")
        return {
            "Trades": time_and_sales["Trade"],
            "Quotes": time_and_sales["Quote"],
            "Depths": market_depth[" "]
        }
    
# this is not my preferred routine, but it is consistent with Erfan's code
class Trades(HighFreq):
    def __init__(self,session,security):
        HighFreq.__init__(self,session,security)
        self.report_type = "Trades"
    
    def request(self,start_date,end_date=None,fieldnames=None):
        return HighFreq.request_trades(self,start_date,end_date,fieldnames)
    
    def get_valid_content(self):
        return HighFreq.get_valid_content(self)["Trades"]
    
class Quotes(HighFreq):
    def __init__(self,session,security):
        HighFreq.__init__(self,session,security)
        self.report_type = "Quotes"
    
    def request(self,start_date,end_date=None,fieldnames=None):
        return HighFreq.request_quotes(self,start_date,end_date,fieldnames)
    
    def get_valid_content(self):
        return HighFreq.get_valid_content(self)["Quotes"]
    
class Depths(HighFreq):
    def __init__(self,session,security):
        HighFreq.__init__(self,session,security)
        self.report_type = "Depths"
    
    def request(self,start_date,end_date=None,fieldnames=None):
        return HighFreq.request_depth(self,start_date,end_date,fieldnames)
    
    def get_valid_content(self):
        return HighFreq.get_valid_content(self)["Depths"]


class IntraDay(Extraction):
    def __init__(self,session,security):
        Extraction.__init__(self,session,security)
        self.report_type = "IntraDay"

        # change condition values
        self.condition["SummaryInterval"] = "OneHour"
        self.condition["DisplaySourceRIC"] = "true"

    def request(self,start_date,end_date=None,fieldnames=None):
        self.odata_type += "TickHistoryIntradaySummariesExtractionRequest"
        if fieldnames is None:
            fieldnames = [
                "High Ask",
                "High Ask Size",
                "High Bid",
                "High Bid Size",
                "Low Ask",
                "Low Ask Size",
                "Low Bid",
                "Low Bid Size",
                "Volume"
            ]
        
        # change instrument values
        chain_res = self.security.historical_chain_resolution(self.session,start_date,end_date)
        self.identifiers["InstrumentIdentifiers"] = chain_res

        return Extraction.request(self,start_date,end_date,fieldnames)
    
    def get_valid_content(self):
        return Extraction.get_valid_content(self,"TickHistoryIntradaySummaries")


class EndOfDay(Extraction):
    def __init__(self,session,security):
        Extraction.__init__(self,session,security)
        self.report_type = "EndOfDay"

        # change instrument validation
        self.identifiers["ValidationOptions"] = {
            "AllowHistoricalInstruments": "true"
        }

        # Erfan uses local time for end of day requests
        self.condition = {
            "ReportDateRangeType": "Range",
        }

    def request(self,start_date,end_date=None,fieldnames=None):
        self.odata_type += "ElektronTimeseriesExtractionRequest"
        if fieldnames is None:
            fieldnames = [
                "Trade Date",
                "RIC",
                "Expiration Date",
                "Last Trading Day",
                "Open",
                "Settlement Price",
                "Universal Close Price",
                "Universal Ask Price",
                "Universal Bid Price",
                "Bid",
                "Ask",
                "Volume",
                "Floor Volume",
                "Open Interest"
            ]
        
        # this one runs on local time for some reason
        end_date = start_date if end_date is None else end_date
        return Extraction.localized_request(
            self,
            start_date + "T00:00:00.000000",
            end_date + "T23:59:59.999999",
            fieldnames
        )
    
    def get_valid_content(self):
        return Extraction.get_valid_content(self,"ElektronTimeseries")

## SECURITY CLASS #############################################################

def historical_search(session,ric,start_date,end_date):
    headers = {
        "Authorization": "Token %s" % (session.token),
        "Prefer": "respond-async",
        "Content-Type": "application/json; odata=minimalmetadata"
    }

    # Set the start and end date
    local_start_date = start_date + "T00:00:00.000000" 
    local_end_date   = end_date + "T23:59:59.999999"

    utc_start_date = convert_to_utc(local_start_date,"US/Central")
    utc_end_date   = convert_to_utc(local_end_date,"US/Central")

    data = {
        "Request": {
            "Identifier": ric,
            "Range": {
                "Start": utc_start_date,
                "End": utc_end_date,
            }
        }
    }
    
    json_response = requests.post(
        BASE_URL + "/Search/HistoricalSearch",
        headers = headers,
        data = json.dumps(data),
        proxies = PROXY
    )

    response = json.loads(json_response.text)
    return response["value"]

# instrument search is relatively odd, not really sure how it's useful
def instrument_search(session,ric):
    headers = {
        "Authorization": "Token %s" % (session.token),
        "Prefer": "respond-async",
        "Content-Type": "application/json; odata=minimalmetadata"
    }

    data = {
        "SearchRequest": {
            "InstrumentTypeGroups": [
                "CollatetizedMortgageObligations",
                "Commodities",
                "Equities",
                "FuturesAndOptions",
                "GovCorp",
                "MortgageBackedSecurities",
                "Money",
                "Municipals",
                "Funds"
            ],
            "IdentifierType": "UnderlyingRIC",
            "Identifier": ric,
            "PreferredIdentifierType": "UnderlyingRIC"
        }
    }

    json_response = requests.post(
        BASE_URL + "/Search/InstrumentSearch",
        headers = headers,
        data = json.dumps(data),
        proxies = PROXY
    )

    response = json.loads(json_response.text)
    return response["value"]

class Security:
    chain_rics = ""
    ric_type = "Ric"
    timezone = None

    def __init__(self,base_ric):
        self.base_ric = base_ric
    
    def historical_chain_resolution(self,session,start_date,end_date=None):
        end_date = start_date if end_date is None else end_date

        headers = {
            "Authorization": "Token %s" % (session.token),
            "Content-Type": "application/json",
            "Accept-Charset": "UTF-8",
            "Prefer": "respond-async, wait=1",
        }

        # Set the start and end date
        local_start_date = start_date + "T00:00:00.000000" 
        local_end_date   = end_date + "T23:59:59.999999"

        utc_start_date = convert_to_utc(local_start_date,self.timezone)
        utc_end_date   = convert_to_utc(local_end_date,self.timezone)

        data = {
            "Request": {
                "ChainRics": [self.chain_rics, ],
                "Range": {
                    "Start": utc_start_date,
                    "End": utc_end_date,
                }
            }
        }

        json_response = requests.post(
            BASE_URL + "/Search/HistoricalChainResolution",
            headers = headers,
            data = json.dumps(data),
            proxies = PROXY
        )
        
        response = json.loads(json_response.text)
        identifier_list = []
        for item in response["value"][0]["Constituents"]:
            identifier_list.append({"Identifier": item["Identifier"],"IdentifierType": "Ric"})

        return identifier_list
    
class Futures(Security):
    def __init__(self,base_ric,timezone):
        Security.__init__(self,base_ric)

        if base_ric != "VX:VE":
            self.chain_rics = "0#" + base_ric + ":"
        elif base_ric == "VX:VE":
            self.chain_rics = "0#" + base_ric

        self.ric_type = "ChainRIC"
        self.timezone = timezone

class Equity(Security):
    def __init__(self,base_ric,timezone):
        Security.__init__(self,base_ric)

        self.chain_rics = base_ric
        self.timezone = timezone

class Options(Security):
    def __init__(self,base_ric,timezone):
        Security.__init__(self,base_ric)

        self.chain_rics = "0#" + base_ric + "*.U"
        self.timezone = timezone

class Treasury(Security):
    def __init__(self,base_ric,timezone):
        Security.__init__(self,base_ric)

        self.chain_rics = "0#" + base_ric + "=R"
        self.timezone = timezone

class FixedIncome(Security):
    def __init__(self,base_ric,timezone):
        Security.__init__(self,base_ric)

        self.chain_rics = base_ric
        self.ric_type = "ChainRIC"
        self.timezone = timezone


## UTILITIES ##################################################################

# for reading status reports and requests
def md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename,"rb") as f:
        for chunk in iter(lambda: f.read(4096),b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def convert_to_utc(str_date,timezone):
    date_time  = pd.to_datetime(str_date)
    local_time = date_time.tz_localize(timezone)
    utc_time   = local_time.tz_convert(None)

    str_utc = utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return str_utc[0:-3] + "+00:00"