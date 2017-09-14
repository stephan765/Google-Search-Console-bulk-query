import webbrowser
import csv
import json
import time
import os
import sys
from datetime import datetime, timedelta
import itertools
import httplib2
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))

WEBMASTER_CREDENTIALS_FILE_PATH = CURRENT_DIR+"\\webmaster_credentials.dat"

QUERY_REQUIRES = [
    "property_uri",
    "start_date",
    "end_date"
]


def rate_limit(max_per_minute):
    """Decorator function to prevent more than x calls per minute of any function"""
    min_interval = 60.0 / float(max_per_minute)

    def decorate(func):
        last_time_called = [0.0]

        def rate_limited_function(*args, **kwargs):
            elapsed = time.clock() - last_time_called[0]
            wait_for = min_interval - elapsed
            if wait_for > 0:
                time.sleep(wait_for)
            ret = func(*args, **kwargs)
            last_time_called[0] = time.clock()
            return ret
        return rate_limited_function
    return decorate


def acquire_new_oauth2_credentials(secrets_file):
    """
    secrets_file: A JSON file containing:
        client_id
        client_secret
        redirect_uris
        auth_uri
        token_uri
    returns:
        credentials for use with Google APIs
    """
    flow = flow_from_clientsecrets(
        secrets_file,
        scope="https://www.googleapis.com/auth/webmasters.readonly",
        redirect_uri="urn:ietf:wg:oauth:2.0:oob")
    auth_uri = flow.step1_get_authorize_url()
    webbrowser.open(auth_uri)
    print("Please enter the following URL in a browser "+auth_uri)
    auth_code = input("Enter the authentication code: ")
    credentials = flow.step2_exchange(auth_code)
    return credentials


def load_oauth2_credentials(secrets_file):
    """
    Looks for a credentials file first.
    If one does not exist, calls a function to acquire and save new credentials.
    """
    storage = Storage(WEBMASTER_CREDENTIALS_FILE_PATH)
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = acquire_new_oauth2_credentials(secrets_file)
    storage.put(credentials)
    return credentials


def create_search_console_client(credentials):
    """The search console client allows us to perform queries against the API.
    To create it, pass in your already authenticated credentials
    credentials:
        An object representing Google API credentials.
    """
    http_auth = httplib2.Http()
    http_auth = credentials.authorize(http_auth)
    service = build('webmasters', 'v3', http=http_auth)
    return service


def date_range(date1, date2, delta=timedelta(days=1)):
    """
    Args:
        date1: The datetime object representing the first day in the range.
        date2: The datetime object representing the second day in the range.
        delta: A datetime.timedelta instance, specifying the step interval. Defaults to one day.
    Yields:
        Each datetime object in the range.
    """
    current_date = date1
    while current_date <= date2:
        yield current_date
        current_date += delta


def generate_filters(**kwargs):
    """Yields a filter list for each combination of the args provided."""
    kwargs = dict((k, v) for k, v in kwargs.items() if v)
    dimensions = kwargs.keys()
    values = list(kwargs.values())
    for vals in itertools.product(*values):
        yield [{
            'dimension': dim,
            'operator': 'equals',
            'expression': val} for dim, val in zip(dimensions, vals)
              ]


@rate_limit(200)
def execute_request(service, property_uri, request, max_retries=5, wait_interval=4,
                    retry_errors=(503, 500)):
    """Executes a searchanalytics request.
    Args:
        service: The webmasters service object/client to use for execution.
        property_uri: Matches the URI in Google Search Console.
        request: The request to be executed.
    Returns:
        An array of response rows.
    """

    #   keep trying if request fails due to 503 - service unavailable
    response = None
    retries = 0
    while retries <= max_retries:
        try:
            response = service.searchanalytics().query(siteUrl=property_uri, body=request).execute()
            break
        except HttpError as e:
            decoded_error_body = e.content.decode('utf-8')
            json_error = json.loads(decoded_error_body)
            json_error['error']['code']

            if json_error['error']['code'] in retry_errors:
                time.sleep(wait_interval)
                retries += 1
                continue
            else:
                break
        else:
            break

    return response


def validate_google_query_dict(dict):
    valid = True
    for key in QUERY_REQUIRES:
        if key not in dict:
            print("Your google query params needs to include {}".format(key))
            valid = False

    return valid


def main(google_sc_query_params):
    '''
    This script needs to be passed a dictionary of parameters which the following 
    information (some optional):

    property_uri (REQUIRED): The property URI to query. Must exactly match a property URI in Google Search Console
    start_date (REQUIRED): The start date to begin gathering data from. Format: YYYY-MM-DD
    end_date (REQUIRED): The end date to stop gathering data at. Format: YYYY-MM-DD
    output_location: The folder output location of the script. Using "." will select the current folder.
    max_rows_per_day: This is the maximum number of rows the search console API will return in a single
    call (tops out at 5000).
    pages: The location of a list of pages to download information for. New line separators for each page required.
    Without this the script will download information for the property as a whole.
    devices: A list of the devices you want as dimensions (desktop, tablet and smartphone are options.)
    countries: A list of the countries you want as dimensions.
    url_type: A prefix to add to the end of each of the generated CSV files

    The API credentials must be contained in the same directory as the script with the file name "credentials.json"

    '''
    if validate_google_query_dict(google_sc_query_params) is False:
        return("You need a fully featured dictionary.")

    settings = google_sc_query_params
    settings['secrets_file'] = CURRENT_DIR+'\\credentials.json'

    # Read page filters, if any.
    if settings["pages"] == "":
        pages = []
    else:
        with open(settings["pages"], "r") as file_handle:
            # check if property_uri is in 
            # can't work out how to do one line comprehension of this
            # line if settings["property_uri not in line for line in file_handle.readlines()
            pages = []
            for line in file_handle.readlines():
                if settings["property_uri"] in line:
                    pages.append(line.strip("\n"))
                else:
                    sys.exit("Your page list contains pages which don't have the GSC property in the URL. GSC API needs full URLs not just paths.")

    # Check if user has remembered to put trailing slash on path
    # if not add
    if settings["output_location"]:
        if settings["output_location"][-1:] != "/":
            settings["output_location"] = settings["output_location"]+"/"

    # Prepare the API service
    credentials = load_oauth2_credentials(settings["secrets_file"])
    service = create_search_console_client(credentials)

    # Convert date strings to datetime objects
    start_date = datetime.strptime(settings["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(settings["end_date"], "%Y-%m-%d")

    # Loop through the date range
    for day in date_range(start_date, end_date):

        output_file = settings["output_location"]+settings["url_type"]+"_{}.csv".format(day.strftime("%Y%m%d"))
        day = day.strftime("%Y-%m-%d")
        output_rows = []
        for filter_set in generate_filters(page=pages, device=settings["devices"], country=settings["countries"]):

            request = {
                'startDate': day,
                'endDate': day,
                'dimensions': ['query'],
                'rowLimit': settings["max_rows_per_day"],
                'dimensionFilterGroups': [
                    {
                        "groupType": "and",
                        "filters": filter_set
                    }
                ]
            }
            response = execute_request(service, settings["property_uri"], request)

            if response is None:

                print("Request failed for "+request['dimensionFilterGroups'][0]['filters'][0]['expression']+
                      ". Device: " + request['dimensionFilterGroups'][0]['filters'][1]['expression'] + 
                      ". Country: " + request['dimensionFilterGroups'][0]['filters'][2]['expression'])
                # skip rest of loop
                continue              

            if 'rows' in response:
                # filters must be identical order for db loading
                # if single value exists in arg.pages, then page must be set manually as not generated
                if len(settings["pages"]) == 1:
                    filters = [settings["pages"][0],'worldwide','all_devices', settings["url_type"]]
                else:
                    filters = ['gsc_property','worldwide','all_devices', settings["url_type"]]

                for f in filter_set:
                    if(f['dimension'] == 'page'):
                        filters[0] = f['expression']
                    elif(f['dimension'] == 'country'):
                        filters[1] = f['expression']
                    elif(f['dimension'] == 'device'):
                        filters[2] = f['expression']

                for row in response['rows']:
                    keys = ','.join(row['keys'])
                    output_row = [keys, row['clicks'], row['impressions'], row['ctr'], row['position']]
                    output_row.extend(filters)
                    output_row.append(day)
                    output_rows.append(output_row)

        with open(output_file, 'w', newline="", encoding="utf-8-sig") as file_handle:
            csvwriter = csv.writer(file_handle)
            csvwriter.writerows(output_rows)
        print("query for "+day+" has been run")


if __name__ == '__main__':
    main()
