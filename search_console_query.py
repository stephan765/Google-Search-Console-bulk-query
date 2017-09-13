import webbrowser
import csv
import json
import time
import sys
from datetime import datetime, timedelta
import itertools
import argparse
import httplib2
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

WEBMASTER_CREDENTIALS_FILE_PATH = "webmaster_credentials.dat"


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
        redirect_uri="http://localhost")
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
            'expression': val} for dim, val in zip(dimensions, vals)]


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

    # keep trying if request fails due to 503 - service unavailable
    response=None
    retries = 0
    while retries <= max_retries:
        try:
            response = service.searchanalytics().query(siteUrl=property_uri, body=request).execute()
            break
        except HttpError as e:
            decoded_error_body = e.content.decode('utf-8')
            json_error = json.loads(decoded_error_body)

            if json_error['error']['code'] in retry_errors:
                time.sleep(wait_interval)
                retries += 1
                continue
            else:
                break
        else:
            break

    return response


def parse_command_line_options():
    """Parses arguments from the command line and returns them in the form of an ArgParser object."""
    parser = argparse.ArgumentParser(description="Query the Google Search Console API for every day in a date range.")
    parser.add_argument('property_uri', type=str, help='The property URI to query. Must exactly match a property URI in Google Search Console')
    parser.add_argument('start_date', type=str, help='The start date for the query. Should not be more than 90 days ago')
    parser.add_argument('end_date', type=str, help='The last date to query. Should not be sooner than two days ago.')
    parser.add_argument('--secrets_file', type=str, default='credentials.json', help='File path of your Google Client ID and Client Secret')
    parser.add_argument('--config_file', type=str, help='File path of a config file containing settings for this Search Console property.')
    parser.add_argument('--output_location', type=str, help='The folder output location of the script -- This will add trailing slash if forgotten', default="")
    parser.add_argument('--url_type', type=str, help='A string to add to the beginning of the file', default="")
    parser.add_argument('--max-rows-per-day', '-n', type=int, default=5000, help='The maximum number of rows to return for each day in the range')

    filters = parser.add_argument_group('filters')
    filters.add_argument('--pages', type=str, help='File path of a CSV list of pages to filter by', default="")
    filters.add_argument('--devices', nargs='*', type=str, help='List of devices to filter by. By default we do segment by device.',
                         default=['mobile', 'desktop', 'tablet'])
    filters.add_argument('--countries', nargs='*', type=str, help='List of countries to filter by', default=[])
    return parser.parse_args()


def main():
    args = parse_command_line_options()
    # Read page filters, if any.
    if args.pages == "":
        pages = []
    else:
        with open(args.pages, "r") as file_handle:
            pages = []
            for line in file_handle.readlines():

    # Prepare the API service
    credentials = load_oauth2_credentials(args.secrets_file)
    service = create_search_console_client(credentials)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Loop through the date range
    for day in date_range(start_date, end_date):
        output_file = args.output_location+args.url_type+"_{}.csv".format(day.strftime("%Y%m%d"))
        day = day.strftime("%Y-%m-%d")
        output_rows = []
        for filter_set in generate_filters(device=args.devices, country=args.countries):
            pagination_needed = True
            pagination_counter = 0
            while pagination_needed:
                request = {
                    'startDate': day,
                    'endDate': day,
                    'dimensions': ['page','query'],
                    'rowLimit': args.max_rows_per_day,
                    'dimensionFilterGroups': [
                        {
                            "groupType": "and",
                            "filters": filter_set
                        }
                    ],
                    'startRow': pagination_counter*5000,
                    'aggregationType': "byPage"
                }

                response = execute_request(service, args.property_uri, request)
                if response is None:
                    print("Request returned nothing "+json.dumps(request, indent=2))
                    # skip rest of loop
                    break

                if response['responseAggregationType'] == 'byPage':
                    if 'rows' in response:
                        # Check if we need to stop pagination
                        if len(response['rows']) < 5000:
                            pagination_needed = False
                        else:
                            pagination_counter += 1

                        # filters must be identical order for db loading
                        # if single value exists in arg.pages, then page must be set manually as not generated
                        filters = ['worldwide','all_devices']

                        for f in filter_set:
                            if(f['dimension'] == 'country'):
                                filters[0] = f['expression']
                            elif(f['dimension'] == 'device'):
                                filters[1] = f['expression']

                        for row in response['rows']:
                            output_row = row['keys']
                            output_row.extend([row['clicks'], row['impressions'], row['ctr'], row['position']])
                            output_row.extend(filters)
                            output_rows.append(output_row)
                    else:
                        print("There is no search console for the" + day)
                        break

        with open(output_file, 'w', newline="", encoding="utf-8-sig") as file_handle:
            csvwriter = csv.writer(file_handle)
            csvwriter.writerows(output_rows)
        print(args.property_uri + " query for "+day+" has been run")


if __name__ == '__main__':
    main()
