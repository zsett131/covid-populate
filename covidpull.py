import argparse
import requests
import camelot
import csv
import dateutil
import datetime
from exceptions import *

VERBOSE = False

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

def process(mode, source, cases, deaths, output, date=None):
    procClass = mode2class.get(mode, CSVReader)
    reader = procClass(source)
    data = reader.read()
    header = data[0]
    values = data[1:]
    if date:
        values.sort(key=getDate, reverse=True)
    verbose(header)
    cases = header.index(cases)
    deaths = header.index(deaths)
    #data = []

def verbose(output):
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
    parser.add_argument('--output-cases', '-oc', type=str, default='Cases', help='output column header for case numbers')
    parser.add_argument('--output-deaths', '-od', type=str, default='Deaths', help='output column header for death numbers')
    parser.add_argument('--counties', type=str, default="County", help="output column for county names")
    parser.add_argument('--date', '-d', type=str, default="Date", help="input date column header name")
    args = parser.parse_args()
    VERBOSE = args.verbose
    verbose(args)

    process(mode=args.mode, source=args.source, cases=args.cases, deaths=args.deaths, output=args.output, date=args.date)

