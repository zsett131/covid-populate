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
def verbose(x): return (print(x) if VERBOSE else None)


class Source:
    def __init__(self, source):
        self.data = None
        self.pull(source)

    def pull(self, source, pages=None):
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
        raise NotImplementedError()

    def pull(self, source, pages):
        tables = camelot.read_pdf(source, pages=pages)
        return self.verify(tables)

    def verify(self, data):
        Source.verify(self, data)
        self.data = data

    def read(self):
        self.data.export('cruft.csv')


class HTMLReader(Source):
    def __init__(self, source):
        Source.__init__(self, source)

    def pull(self, source):
        pass  # TODO

    def verify(self, data):
        Source.verify(self, data)

    def read(self):
        pass  # TODO


mode2class = {'csv': CSVReader,
              'pdf': PDFReader}


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


def process(mode, source, cases, deaths, output, counties, output_cases, output_deaths, pages, date=None, firstdose=None, vaccinated=None, output_firstdose=None, output_vaccinated=None, vaccine_source=None, vaccine_date=None):
    def getDate(x):
        return dateutil.parser.parse(x[date]) if x[date] else datetime.datetime(datetime.MINYEAR, 1, 1)

    def getVaccineDate(x):
        return dateutil.parser.parse(x[vaccine_date]) if x[vaccine_date] else datetime.datetime(datetime.MINYEAR, 1, 1)

    procClass = mode2class.get(mode, CSVReader)
    reader = procClass(source)
    vaccineReader = None
    if vaccine_source:
        vaccineReader = procClass(vaccine_source)
        vaccineData = vaccineReader.read()
        vaccineHeader = vaccineData[0]
        verbose(vaccineHeader)
        vaccineValues = vaccineData[1:]

    data = reader.read()
    header = data[0]
    verbose(header)
    values = data[1:]
    if date:
        date = header.index(date)
        values.sort(key=getDate, reverse=True)
        if vaccineValues and vaccine_date:
            vaccine_date = vaccineHeader.index(vaccine_date)
            vaccineValues.sort(key=getVaccineDate, reverse=True)

    cases = header.index(cases)
    verbose('case index: %d' % cases)
    deaths = header.index(deaths)
    verbose('death index: %d' % deaths)
    if firstdose:
        dose1 = header.index(
            firstdose) if not vaccine_source else vaccineHeader.index(firstdose)
        verbose('first dose index: %d' % dose1)
    if vaccinated:
        vaccinated = header.index(
            vaccinated) if not vaccine_source else vaccineHeader.index(vaccinated)
        verbose('vaccinated index: %d' % vaccinated)
    sheet = authenticate()
    verbose(dir(sheet))

    # Begin setting up for our data update.
    countyData = {}
    sheetHeader = sheet.values().get(
        spreadsheetId=output, range="1:1").execute().get('values', [])

    verbose(sheetHeader)

    if not sheetHeader:
        raise SheetFormatError("The output sheet has an empty header row.")

    if not counties in sheetHeader[0]:
        raise SheetFormatError("Could not find %s in header." % counties)

    # Store county names and indices necessary to populate data.
    iCounties = sheetHeader[0].index(counties)
    iCases = sheetHeader[0].index(output_cases)
    iDeaths = sheetHeader[0].index(output_deaths)
    if firstdose and output_firstdose:
        iDose = sheetHeader[0].index(output_firstdose)
    if vaccinated and output_vaccinated:
        iVaccinated = sheetHeader[0].index(output_vaccinated)
    countyColumn = sheet.values().get(spreadsheetId=output, range='%s2:%s' % (
        LETTERS[iCounties], LETTERS[iCounties])).execute().get('values', [])

    i = 0
    while countyColumn[i]:
        countyData[countyColumn[i][0]] = {
            'cases': 0, 'deaths': 0, 'recovered': 0, 'dose1': 0, 'full': 0}
        i += 1

    encountered = []
    for row in values:
        for column in row:
            if column in encountered:
                continue
            if column in countyData.keys():
                countyData[column]['cases'] = row[cases]
                countyData[column]['deaths'] = row[deaths]
                if not vaccine_source:
                    countyData[column]['dose1'] = row[dose1]
                    countyData[column]['full'] = row[vaccinated]
                encountered.append(column)

    if vaccine_source:
        vaccineEncountered = []
        for row in vaccineValues:
            for column in row:
                if column in vaccineEncountered:
                    continue
                if column in countyData.keys():
                    countyData[column]['dose1'] = row[dose1]
                    countyData[column]['full'] = row[vaccinated]
                    vaccineEncountered.append(column)

    caseValues = [[countyData[county]['cases']]
                  for county in countyData.keys()]
    body = {
        'values': caseValues
    }
    result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (
        LETTERS[iCases], LETTERS[iCases]), valueInputOption="USER_ENTERED", body=body).execute()
    print('cases updated')

    deathValues = [[countyData[county]['deaths']]
                   for county in countyData.keys()]
    body = {
        'values': deathValues
    }
    result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (
        LETTERS[iDeaths], LETTERS[iDeaths]), valueInputOption="USER_ENTERED", body=body).execute()
    print('deaths updated')

    if firstdose and output_firstdose:
        doseValues = [[countyData[county]['dose1']]
                      for county in countyData.keys()]
        body = {
            'values': doseValues
        }
        result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (
            LETTERS[iDose], LETTERS[iDose]), valueInputOption="USER_ENTERED", body=body).execute()
        print('first dose updated')

    if vaccinated and output_vaccinated:
        vaxValues = [[countyData[county]['full']]
                     for county in countyData.keys()]
        body = {
            'values': vaxValues
        }
        result = sheet.values().update(spreadsheetId=output, range='%s2:%s' % (
            LETTERS[iVaccinated], LETTERS[iVaccinated]), valueInputOption="USER_ENTERED", body=body).execute()
        print('vaccines updated')

    verbose(countyData)


if __name__ == '__main__':
    # Script has been invoked as entry level module.
    parser = argparse.ArgumentParser(
        description="Pull COVID-19 case and death data and populate a google sheet.")
    parser.add_argument('mode', type=str, default='csv',
                        help="set format of data source (default: sheet)")
    parser.add_argument('source', type=str, help="data source URL")
    parser.add_argument('cases', type=str,
                        help="input case column header name")
    parser.add_argument('deaths', type=str,
                        help="input death column header name")
    parser.add_argument('output', type=str,
                        help="output google sheet for your data")
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="enable verbose logging")
    parser.add_argument('--output-cases', '-oc', type=str,
                        default='CONFIRMED', help='output column header for case numbers')
    parser.add_argument('--output-deaths', '-od', type=str,
                        default='DEATHS', help='output column header for death numbers')
    parser.add_argument('--counties', type=str, default="COUNTIES",
                        help="output column for county names")
    parser.add_argument('--date', '-d', type=str,
                        default="Date", help="input date column header name")
    parser.add_argument('--pages', '-p', type=str, default='1-end',
                        help="specify the pages to be read from the pdf")
    parser.add_argument('--firstdose', '-fd', type=str,
                        help="input first dose vaccine column")
    parser.add_argument('--vaccinated', '-vax', type=str,
                        help="input full vaccine column")
    parser.add_argument('--output-firstdose', '-ofd', type=str,
                        help="output first dose vaccine column")
    parser.add_argument('--output-vaccinated', '-ovax',
                        type=str, help="output full vaccine column")
    parser.add_argument('--vaccine-source', '-vs', type=str,
                        help="optional data source for separate vaccine data")
    parser.add_argument('--vaccine-date', '-vd', type=str,
                        help="input vaccine date column header name")
    args = parser.parse_args()
    VERBOSE = args.verbose
    verbose(args)

    process(mode=args.mode, source=args.source, cases=args.cases, deaths=args.deaths, output=args.output, counties=args.counties,
            output_cases=args.output_cases, output_deaths=args.output_deaths, pages=args.pages, date=args.date, firstdose=args.firstdose, vaccinated=args.vaccinated, output_firstdose=args.output_firstdose, output_vaccinated=args.output_vaccinated, vaccine_source=args.vaccine_source, vaccine_date=args.vaccine_date)
