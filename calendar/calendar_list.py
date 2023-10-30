from __future__ import print_function

import json
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


def main():
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
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

    try:
        service = build('calendar', 'v3', credentials=creds)

        # Call the Calendar API
        calendar_list_result = service.calendarList().list().execute()
        calendars = calendar_list_result.get('items', [])

        if not calendars:
            print('No calendars found')
        
        cal_id = None
        cal_name = None
        for calendar in calendars:
            cal_name = calendar.get('summary').replace(' ', '_').lower()
            cal_id = calendar.get('id')
            get_events(cal_id, cal_name, service)

    except HttpError as error:
        print('An error occurred: %s' % error)


def get_events(cal_id, cal_name, service):
    print('Getting single calendar events')
    next_page = None
    i = 0
    while True:
        if cal_name == 'hannay_family':
            break
        
        i = i + 1
        events_result = service.events().list(calendarId=cal_id, showHiddenInvitations=True, maxResults=40,
                                            singleEvents=True,
                                            showDeleted=True, timeZone='Australia/Perth', pageToken=next_page).execute()
        
        json_title = "{}_single-events_{}.json".format(cal_name, i)
        with open(json_title, 'w') as f:
            json.dump(events_result, f)

        if events_result.get('nextPageToken'):
            next_page = events_result.get('nextPageToken')
        else:
            break

    print('Getting non-single calendar events')
    next_page = None
    i = 0
    while True:
        i = i + 1
        events_result = service.events().list(calendarId=cal_id, showHiddenInvitations=True, maxResults=40,
                                            showDeleted=True, timeZone='Australia/Perth', pageToken=next_page).execute()
        
        json_title = "{}_events_{}.json".format(cal_name, i)
        with open(json_title, 'w') as f:
            json.dump(events_result, f)

        if events_result.get('nextPageToken'):
            next_page = events_result.get('nextPageToken')
        else:
            break



if __name__ == '__main__':
    main()
