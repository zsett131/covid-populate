import argparse
import requests
import camelot
import csv
import dateutil
import datetime
from exceptions import *
import string

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
LETTERS = list(string.ascii_uppercase)

VERBOSE = False
verbose = lambda x: (print(x) if VERBOSE else None)

class Source:
    def __init__(self, source):
        self.data = None
        self.pull(source)
       
    def pull(self, source):
        r = requests.get(source, allow_redirects=True)
        data = r.content
        return self.verify(data)

    def verify(self, data):
        """
        Implement and inherit in child classes.
        """
        if not data:
            raise DataNotFoundError("Your source doesn't contain any data.")

    def read(self):
        """
        Virtual method; override in child classes.
        """
        raise NotImplementedError()


class CSVReader(Source):
    def __init__(self, source):
        Source.__init__(self, source)

    def verify(self, data):
        Source.verify(self, data)
        self.data = data
    
    def read(self):
        decoded = self.data.decode('utf-8')
        cr = csv.reader(decoded.splitlines(), delimiter=',')
        table = list(cr)
        return table

class ExcelReader(Source):
    def __init__(self, source):
        Source.__init__(self, source)
    
    def verify(self, data):
        pass


    def read(self):
        pass

class PDFReader(Source):
    def __init__(self, source):
        Source.__init__(self, source)

    def verify(self, data):
        Source.verify(self, data)
        self.data = data
    
    def read(self):
        pass # TODO

mode2class = {'csv': CSVReader,
              'pdf': PDFReader}

def getDate(x):
    return dateutil.parser.parse(x[0]) if x[0] else datetime.datetime(datetime.MINYEAR, 1, 1)

def authenticate():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)
    
    # Call the Sheets API
    sheet = service.spreadsheets()
    return sheet

def process(mode, source, cases, deaths, output, counties, output_cases, output_deaths, date=None):
    procClass = mode2class.get(mode, CSVReader)
    reader = procClass(source)
    data = reader.read()
    header = data[0]
    values = data[1:]
    if date:
        values.sort(key=getDate, reverse=True)
    verbose(header)
    cases = header.index(cases)
    verbose('case index: %d' % cases)
    deaths = header.index(deaths)
    verbose('death index: %d' % deaths)
    sheet = authenticate()
    verbose(dir(sheet))
    
    # Begin setting up for our data update.
    countyData = {}
    sheetHeader = sheet.values().get(spreadsheetId=output, range="1:1").execute().get('values', [])
    
    verbose(sheetHeader)

    if not sheetHeader:
        raise SheetFormatError("The output sheet has an empty header row.")
    
    if not counties in sheetHeader[0]:
        raise SheetFormatError("Could not find %s in header." % counties)
    
    iCounties = sheetHeader[0].index(counties)
    iCases = sheetHeader[0].index(output_cases)
    iDeaths = sheetHeader[0].index(output_deaths)
    countyColumn = sheet.values().get(spreadsheetId=output, range='%s2:%s' % (LETTERS[iCounties], LETTERS[iCounties])).execute().get('values', [])
    
    i = 0
    while countyColumn[i]:
        countyData[countyColumn[i][0]] = {'cases': 0, 'deaths': 0, 'recovered': 0, 'dose1': 0, 'full': 0}
        i += 1
    
    encountered = []
    for row in values:
        for column in row:
            if column in encountered:
                continue
            if column in countyData.keys():
                countyData[column]['cases'] = row[cases]
                countyData[column]['deaths'] = row[deaths]
                encountered.append(column)
    
    caseValues = [[countyData[county]['cases']] for county in countyData.keys()]
    body = {
        'values': caseValues
    }
    result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (LETTERS[iCases], LETTERS[iCases]), valueInputOption="USER_ENTERED", body=body).execute()
    print('cases updated')

    deathValues = [[countyData[county]['deaths']] for county in countyData.keys()]
    body = {
        'values': deathValues
    }
    result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (LETTERS[iDeaths], LETTERS[iDeaths]), valueInputOption="USER_ENTERED", body=body).execute()
    print('deaths updated')

    verbose(countyData)

def verbose1(output):
    if VERBOSE:
        print(output)

if __name__ == '__main__':
    # Script has been invoked as entry level module.
    parser = argparse.ArgumentParser(description="Pull COVID-19 case and death data and populate a google sheet.")
    parser.add_argument('mode', type=str, default='csv', help="set format of data source (default: sheet)")
    parser.add_argument('source', type=str, help="data source URL")
    parser.add_argument('cases', type=str, help="input case column header name")
    parser.add_argument('deaths', type=str, help="input death column header name")
    parser.add_argument('output', type=str, help="output google sheet for your data")
    parser.add_argument('--verbose', '-v', action='store_true', help="enable verbose logging")
    parser.add_argument('--output-cases', '-oc', type=str, default='CONFIRMED', help='output column header for case numbers')
    parser.add_argument('--output-deaths', '-od', type=str, default='DEATHS', help='output column header for death numbers')
    parser.add_argument('--counties', type=str, default="COUNTIES", help="output column for county names")
    parser.add_argument('--date', '-d', type=str, default="Date", help="input date column header name")
    args = parser.parse_args()
    VERBOSE = args.verbose
    verbose(args)

    process(mode=args.mode, source=args.source, cases=args.cases, deaths=args.deaths, output=args.output, counties=args.counties, output_cases=args.output_cases, output_deaths=args.output_deaths, date=args.date)

