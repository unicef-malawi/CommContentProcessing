# CommContentProcessing.py
#
# Olivier Demars - olivier@odc.services
# for UNICEF Malawi
#
# April 2019

# Standard libraries
import os
import sys
import os.path
import json
import logging
import base64
import re
import time
import pickle
from datetime import datetime

# Libraries to be installed with pip
import requests
import pandas
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup
from arcgis.gis import GIS
from pyproj import Proj, transform
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

# Variable file
from CommContentProcessingVariables import *


def get_data(service_url, token):
    payload = {
        'token': token,
        'f': 'json'
    }

    feature_response = requests.get(service_url, params=payload)

    json_string = feature_response.text
    pydict = json.loads(json_string)

    return pydict


def google_service_init(api, version, scope, pickle_file, credentials_file):

    creds = None
    # The pickle file  stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, scope)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(pickle_file, 'wb') as token:
            pickle.dump(creds, token)

    return build(api, version, credentials=creds)


def update_google_sheet(sheet, sheet_content, sheet_id_list, content_list):

    counter_processed = 0
    counter_added = 0
    counter_updated = 0
    counter_unchanged = 0

    for content in content_list:

        # if len(content) < ONLINE_CONTENT_SORT_END_COLUMN_INDEX - 1:
        #     content.append('0') # add latitude 0 for YouTube
        #     content.append('0') # add longitude 0 for YouTube

        value_range_body = {
            "range": ONLINE_CONTENT_RANGE_NAME,
            "majorDimension": 'ROWS',
            "values": [
                content
            ]
        }

        # Post ID is the second column in the file
        unique_id = content[1]

        # If new information append to list
        if unique_id not in sheet_id_list:
            response = sheet.values().append(spreadsheetId=ONLINE_CONTENT_SPREADSHEET_ID,
                                             range=ONLINE_CONTENT_RANGE_NAME,
                                             valueInputOption='RAW',
                                             insertDataOption='INSERT_ROWS',
                                             body=value_range_body).execute()
            counter_added = counter_added + 1

            logging.info('Post id %s added' % content[1])
            print('Post id %s added' % content[1])

        # If already existing information, update content
        else:
            # Row to update: add 2 to account for header and index difference between list and sheet
            row_to_update = sheet_id_list.index(unique_id) + 2

            current_values = sheet_content[sheet_id_list.index(unique_id)][0:len(content)]

            if current_values != content:
                update_range = ONLINE_CONTENT_UPDATE_RANGE % (row_to_update, row_to_update)
                value_range_body['range'] = update_range

                response = sheet.values().update(spreadsheetId=ONLINE_CONTENT_SPREADSHEET_ID,
                                                 range=update_range,
                                                 valueInputOption='RAW',
                                                 body=value_range_body).execute()

                counter_updated = counter_updated + 1

                logging.info('Post id %s updated' % current_values[1])
                for idx, val in enumerate(current_values):
                    if val != content[idx]:
                        logging.info('Field %s updated' % str(idx))
                        logging.info('Old value: %s' % val)
                        logging.info('New value: %s' % content[idx])

                print('Post id %s updated' % current_values[1])

            else:
                counter_unchanged = counter_unchanged + 1
                # print('ID already exists and same information, No update')

        counter_processed = counter_processed + 1

    logging.info('Processed: %s, Added: %s, Updated: %s, Unchanged: %s'
                 % (counter_processed, counter_added, counter_updated, counter_unchanged))
    print('Processed: %s, Added: %s, Updated: %s, Unchanged: %s'
          % (counter_processed, counter_added, counter_updated, counter_unchanged))


def process_tchop(dump):

    content_list = []

    for mix in dump:

        # mix_id = mix['id']
        # mix_title = mix['title']
        # mix_subtitle = mix['subtitle']

        for card in mix['cards']:

            # print(card)
            card_id = str(card['id'])
            card_type = card['type']
            card_posted_time = card['postedTime'][0:19]

            card_url = ''
            card_video_url = ''
            card_image_url = ''
            card_title = ''
            card_headline = ''
            card_text = ''

            if card_type in ['image', 'video']:
                card_title = card['title']
                if card_title is None:
                    card_title = ''

                card_headline = card['headline']

                card_text = card['text']
                if card_text is None:
                    card_text = ''

                if card_type == 'image':
                    card_image_url = card['image']['phone']['jpg']
                elif card_type == 'video':
                    card_video_url = card['video']['url']
                card_exif = card[card_type]['exif']

            elif card_type == 'quote':
                card_url = card['url']
                card_headline = card['quotePerson']
                card_text = card['quote']

                if 'image' in card:
                    card_image_url = card['image']['phone']['jpg']
                    card_exif = card['image']['exif']
                else:
                    card_image_url = ''
                    card_exif = ''

            elif card_type == 'article':
                card_url = card['url']
                card_headline = card['title']
                card_text = card['abstract']

                if 'image' in card:
                    card_image_url = card['image']['phone']['jpg']
                    card_exif = card['image']['exif']
                else:
                    card_image_url = ''
                    card_exif = ''

            # print('id: %s, type: %s, headline: %s' % (card_id, card_type, card_headline))

            if len(card_title) > len(card_text):
                card_text = card_title

            # print(card_id)
            # print(len(card_exif))

            if len(card_exif) == 1:
                latitude = card_exif['gps']['latitude']
                longitude = card_exif['gps']['longitude']
            else:
                latitude = 0
                longitude = 0

            # change card type from image to photo
            if card_type == 'image':
                card_type = 'photo'

            downloaded_values = [
                'Tchop Download Script',                    # __PowerAppsId__
                card_id,                                    # post_id
                card_posted_time,                           # published_date
                card_url,                                   # post_url
                re.sub('[^\x00-\x7f]', '', card_headline),  # title
                re.sub('[^\x00-\x7f]', '', card_text),      # content
                card_image_url,                             # photo_url
                card_video_url,                             # video_url
                card_image_url,                             # thumb_url
                card_type,                                  # type
                'Tchop',                                    # source
                str(round(latitude, 7)),                    # latitude
                str(round(longitude, 7))                    # longitude
                ]

            content_list.append(downloaded_values)

    return content_list


def process_youtube(youtube, channel_id):

    content_list = []

    # Retrieve the list of videos uploaded to the Unicef Malawi channel.
    search_list_request = youtube.search().list(
        channelId=channel_id,
        part="snippet,id",
        type='video',
        maxResults=50
    )

    while search_list_request:
        search_list_response = search_list_request.execute()

        # Print information about each video.
        for playlist_item in search_list_response["items"]:

            video_id = playlist_item["id"]["videoId"]
            post_url = 'https://www.youtube.com/watch?v=%s' % video_id
            video_url = 'https://www.youtube.com/embed/%s?wmode=opaque#isVideo' % video_id
            title = playlist_item["snippet"]["title"]
            thumbnail = playlist_item["snippet"]["thumbnails"]["medium"]["url"]
            description = playlist_item["snippet"]["description"]
            published_date = playlist_item["snippet"]["publishedAt"][0:19]

            # location_info = youtube.videos().list(part='recordingDetails', id=video_id).execute()
            # try:
            #     latitude = location_info["recordingDetails"]["location"]["latitude"]
            #     longitude = location_info["recordingDetails"]["location"]["longitude"]
            #     # print(latitude, longitude)
            # except:
            #     # print('NO LOCATION INFORMATION AVAILABLE')
            #     latitude = '0'
            #     longitude = '0'

            # Latitude/longitude will never be available from YouTube
            downloaded_values = [
                'YouTube Download Script',  # __PowerAppsId__
                video_id,                   # post_id
                published_date,             # published_date
                post_url,                   # post_url
                title,                      # title
                description,                # content
                '',                         # photo_url
                video_url,                  # video_url
                thumbnail,                  # thumb_url
                'video',                    # type
                'YouTube'                   # source
            ]

            content_list.append(downloaded_values)

        search_list_request = youtube.playlistItems().list_next(search_list_request, search_list_response)

    return content_list


def process_blogger(blogger, blog_id):

    content_list = []

    posts = blogger.posts()

    request = posts.list(blogId=blog_id, maxResults=50)
    while request is not None:
        posts_doc = request.execute()

        if 'items' in posts_doc and not (posts_doc['items'] is None):
            for post in posts_doc['items']:

                post_id = post['id']
                published_date = post['published'][0:19]
                post_url = post['url']
                title = post['title']

                soup_blog_content = BeautifulSoup(post['content'], 'html.parser')

                content = soup_blog_content.get_text()[:150].replace('\n', ' ') + '...'

                if len(soup_blog_content.select('a[href]')) > 0:
                    photo_url = soup_blog_content.find_all('a')[0].get('href')
                    thumb_url = soup_blog_content.find_all('a')[0].get('href')
                    video_url = ''
                    post_type = 'photo'
                elif len(soup_blog_content.select('iframe[src]')) > 0:
                    video_url = soup_blog_content.find_all('iframe')[0].get('src')
                    thumb_url = soup_blog_content.find_all('iframe')[0].get('data-thumbnail-src')
                    photo_url = ''
                    post_type = 'video'
                else:
                    video_url = 'NO INFO'
                    thumb_url = 'NO INFO'
                    photo_url = 'NO INFO'
                    post_type = 'no_info'

                try:
                    latitude = post['location']['lat']
                    longitude = post['location']['lng']
                except:
                    latitude = 0
                    longitude = 0

                downloaded_values = [
                    'Blogger Download Script',  # __PowerAppsId__
                    post_id,                    # post_id
                    published_date,             # published_date
                    post_url,                   # post_url
                    title,                      # title
                    content.lstrip(),           # content
                    photo_url,                  # photo_url
                    video_url,                  # video_url
                    thumb_url,                  # thumb_url
                    post_type,                  # type
                    'Blogger',                  # source
                    str(round(latitude, 7)),    # latitude
                    str(round(longitude, 7))    # longitude
                ]

                content_list.append(downloaded_values)

        request = posts.list_next(request, posts_doc)

    return content_list


def export_location_table():

    try:
        logging.info('Start automated Chrome browser')
        chrome_options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": WORDPRESS_LOCATION_EXPORT_DIR}
        chrome_options.add_experimental_option("prefs", prefs)

        browser = webdriver.Chrome(chrome_options=chrome_options)
        browser.get(WORDPRESS_LOCATION_TABLE_URL)
        browser.implicitly_wait(2)

        element1 = browser.find_element_by_id('usernameOrEmail')
        element1.send_keys(WORDPRESS_USERNAME)
        element1.send_keys(Keys.RETURN)
        browser.implicitly_wait(2)

        wordpress_password = base64.b64decode(WORDPRESS_PASSWORD).decode("utf-8")

        element2 = browser.find_element_by_id('password')
        element2.send_keys(wordpress_password)
        element2.send_keys(Keys.RETURN)
        browser.implicitly_wait(2)

        element3 = browser.find_element_by_id('export_locations_table')
        element3.click()

        browser.implicitly_wait(5)
        time.sleep(5)

        browser.close()
        logging.info('Close automated Chrome browser')

        for root, dirs, files in os.walk(WORDPRESS_LOCATION_EXPORT_DIR):
            for file in files:
                # print(root, dirs, file)
                if file[0:3] == 'gmw':
                    exported_file = root + '\\' + file
                    os.remove(WORDPRESS_LOCATION_CSV)
                    os.rename(exported_file, WORDPRESS_LOCATION_CSV)
    except:
        logging.error('Wordpress geo coordinate information ould not be retrieved')


def process_wordpress(wordpress):

    # export location table csv
    export_location_table()

    # load lat/long from csv file
    # 2 -> object_id (post_id), 9 -> latitude, 10 -> longitude
    location_csv = pandas.read_csv(WORDPRESS_LOCATION_CSV, index_col=2, usecols=[2, 9, 10])
    wordpress_location = location_csv.to_dict('index')

    content_list = []

    page = 1
    per_page = 50

    response = requests.get(wordpress, params={'page': page, 'per_page': per_page})

    # print(response.status_code)
    # print(response.text)

    while response.status_code == 200:

        pydict = json.loads(response.text)

        for post in pydict:

            post_id = post['id']
            published_date = post['date'][0:19]
            post_url = post['link']
            title = post['title']['rendered']

            # Using the excerpt field for content
            soup_blog_excerpt = BeautifulSoup(post['excerpt']['rendered'], 'html.parser')
            content = soup_blog_excerpt.get_text()[:150].replace('\n', ' ') + '...'

            post_type = 'photo'

            soup_blog_content = BeautifulSoup(post['content']['rendered'], 'html.parser')
            photo_url = soup_blog_content.find_all('img')[0].get('src')
            thumb_url = soup_blog_content.find_all('img')[0].get('src')

            # Get latitude/longitude from csv file
            try:
                latitude = str(round(wordpress_location[post_id]['latitude'], 7))
                longitude = str(round(wordpress_location[post_id]['longitude'], 7))
            except:
                latitude = '0'
                longitude = '0'

            downloaded_values = [
                'Wordpress Download Script',    # __PowerAppsId__
                str(post_id),                   # post_id
                published_date,                 # published_date
                post_url,                       # post_url
                title,                          # title
                content.lstrip(),               # content
                photo_url,                      # photo_url
                '',                             # video_url
                thumb_url,                      # thumb_url
                post_type,                      # type
                'Wordpress',                    # source
                latitude,                       # latitude
                longitude                       # longitude
            ]

            content_list.append(downloaded_values)

        page = page + 1
        response = requests.get(wordpress, params={'page': page, 'per_page': per_page})

    return content_list


def sheet_to_feature(row):

    # No latitude/longitude for YouTube
    if len(row) == ONLINE_CONTENT_SORT_END_COLUMN_INDEX - 1:
        # Transform longitude and latitude from WGS84 to Web Mercator
        # EPSG:4326 -> WGS 84 -- WGS84 - World Geodetic System 1984
        # EPSG:3857 -> WGS 84 / Pseudo-Mercator -- Spherical Mercator
        if row[11] != '0' or row[12] != '0':
            result = transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), float(row[12]), float(row[11]))
            longitude_proj = round(result[0], 2)
            latitude_proj = round(result[1], 2)
            longitude_gcs = float(row[12])
            latitude_gcs = float(row[11])
        else:
            longitude_proj = 0
            latitude_proj = 0
            longitude_gcs = 0
            latitude_gcs = 0

    else:
        # YouTube
        longitude_proj = 0
        latitude_proj = 0
        longitude_gcs = 0
        latitude_gcs = 0

    date_in_seconds = int(datetime(int(row[2][0:4]), int(row[2][5:7]), int(row[2][8:10]),
                                   int(row[2][11:13]), int(row[2][14:16]), int(row[2][17:19])).timestamp()*1000)

    feature = {
        'attributes': {
            'origin': row[0],
            'post_id': row[1],
            'published_date': date_in_seconds,
            'post_url': row[3],
            'title': row[4],
            'content': row[5],
            'photo_url': row[6],
            'video_url': row[7],
            'thumb_url': row[8],
            'post_type': row[9],
            'source': row[10],
            'latitude': latitude_gcs,
            'longitude': longitude_gcs
        },
        'geometry': {
            'x': longitude_proj,
            'y': latitude_proj
        }
    }

    return feature


def main():

    logging.info('#########################')
    logging.info('###   Process Start   ###')
    logging.info('#########################')

    ################
    # Google Sheet #
    ################
    logging.info('##### Google Sheet')
    logging.info('### Connection to Google Sheet')
    print('Connecting to Google Sheet')
    # Google Sheet connection
    sheets = google_service_init(
        'sheets',
        'v4',
        ['https://www.googleapis.com/auth/spreadsheets'],
        'Sheets-token.pickle',
        'Sheets-credentials.json'
    )

    # Call the Sheets API
    online_content = sheets.spreadsheets()
    result = online_content.values().get(spreadsheetId=ONLINE_CONTENT_SPREADSHEET_ID,
                                         range=ONLINE_CONTENT_RANGE_NAME).execute()
    values = result.get('values', [])

    logging.info('### All current values recovered')

    # Put all IDs in a list
    id_list = []
    for value in values:
        id_list.append(value[1])

    logging.info('### Unique ID list created')

    #########
    # Tchop #
    #########
    logging.info('##### Tchop')
    logging.info('### Data dump from Tchop')
    print('Processing Tchop')
    tchop_dump = get_data('https://tchop.io/api/stream/v1/stories', TCHOP_API_TOKEN)

    tchop_content = process_tchop(tchop_dump)

    logging.info('### Google Sheet update')
    print('Updating Google Sheet with Tchop information')
    update_google_sheet(online_content, values, id_list, tchop_content)

    ###########
    # YouTube #
    ###########
    logging.info('##### YouTube')
    youtube_api = google_service_init(
        'youtube',
        'v3',
        ['https://www.googleapis.com/auth/youtube'],
        'YouTube-token.pickle',
        'YouTube-credentials.json'
    )

    # Process Unicel Malawi channel
    logging.info('### Get YouTube content')
    print('Processing YouTube')
    youtube_content = process_youtube(youtube_api, YOUTUBE_CHANNEL_ID)

    logging.info('### Google Sheet update')
    print('Updating Google Sheet with YouTube information')
    update_google_sheet(online_content, values, id_list, youtube_content)

    ###########
    # Blogger #
    ###########
    logging.info('##### Blogger')
    blogger_api = google_service_init(
        'blogger',
        'v3',
        ['https://www.googleapis.com/auth/blogger'],
        'Blogger-token.pickle',
        'Blogger-credentials.json'
    )

    # Process Youth Out Loud - Malawi
    logging.info('### Get Blogger content')
    print('Processing Blogger')
    blogger_content = process_blogger(blogger_api, BLOGGER_BLOG_ID)

    logging.info('### Google Sheet update')
    print('Updating Google Sheet with Blogger information')
    update_google_sheet(online_content, values, id_list, blogger_content)

    #############
    # Wordpress #
    #############
    logging.info('##### Wordpress')
    logging.info('### Get Wordpress content')
    print('Processing Wordpress')
    wordpress_content = process_wordpress(WORDPRESS_API_POSTS)

    logging.info('### Google Sheet update')
    print('Updating Google Sheet with Wordpress information')
    update_google_sheet(online_content, values, id_list, wordpress_content)

    ################################
    # Sort sheet by published date #
    ################################
    sort_request = {
        "requests": [
            {
                "sortRange": {
                    "range": {
                        "sheetId": 0,
                        "startRowIndex": ONLINE_CONTENT_SORT_START_ROW_INDEX,
                        "startColumnIndex": ONLINE_CONTENT_SORT_START_COLUMN_INDEX,
                        "endColumnIndex": ONLINE_CONTENT_SORT_END_COLUMN_INDEX
                    },
                    "sortSpecs": [
                        {
                            "dimensionIndex": ONLINE_CONTENT_SORT_COLUMN,
                            "sortOrder": "ASCENDING"
                        }
                    ]
                }
            }
        ]
    }

    online_content.batchUpdate(spreadsheetId=ONLINE_CONTENT_SPREADSHEET_ID,
                               body=sort_request).execute()

    logging.info('##### Google Sheet sorted')
    print('Google Sheet Sorted')

    #################
    # ArcGIS Portal #
    #################
    logging.info('##### ArcGIS Portal')
    print('Updating ArcGIS Portal')

    # Get new values from spreadsheet
    new_result = online_content.values().get(spreadsheetId=ONLINE_CONTENT_SPREADSHEET_ID,
                                             range=ONLINE_CONTENT_RANGE_NAME).execute()
    new_values = new_result.get('values', [])

    # Connect to Portal
    arcgis_password = base64.b64decode(ARCGIS_PASSWORD).decode("utf-8")

    try:
        gis = GIS(ARCGIS_PORTAL, ARCGIS_USER, arcgis_password)
    except RuntimeError as error:
        logging.error(f'CANNOT CONNECT TO PORTAL: {error}')  # Add exc_info = 1 to log full error
        print('CANNOT CONNECT TO PORTAL', error)
        sys.exit()
    logging.info('### Connected to ArcGIS Portal')

    # Search for the feature layer by name
    # search_query = 'title:' + ARCGIS_FEATURE_LAYER
    # search_result = gis.content.search(search_query)
    # online_content_item = search_result[0]

    # Search for the feature layer by ID
    online_content_item = gis.content.get(ARCGIS_ITEM_ID)

    # Access the item's feature layers

    online_content_layers = online_content_item.layers
    online_content_flayer = online_content_layers[0]

    layer_df = online_content_flayer.query().df

    counter_processed = 0
    counter_added = 0
    counter_updated = 0
    counter_unchanged = 0

    for value in new_values:

        post_id = value[1]

        new_feature = sheet_to_feature(value)

        if len(layer_df[layer_df.post_id == post_id]) == 1:
            modification_list = {}

            existing_feature_info = layer_df[layer_df.post_id == post_id].to_dict('records')[0]

            id_logging_done = False

            for attrib in existing_feature_info:
                if attrib not in ['objectid', 'SHAPE', 'globalid']:
                    if existing_feature_info[attrib] != new_feature['attributes'][attrib]:
                        modification_list[attrib] = new_feature['attributes'][attrib]

                        if not id_logging_done:
                            logging.info('Updating post_id: %s' % post_id)
                            id_logging_done = True

                        logging.info('new %s: %s' % (attrib, new_feature['attributes'][attrib]))
                        logging.info('existing %s: %s' % (attrib, existing_feature_info[attrib]))

                else:
                    if attrib == 'SHAPE':
                        if round(existing_feature_info['SHAPE']['x'], 2) != new_feature['geometry']['x']:
                            modification_list['x'] = new_feature['geometry']['x']

                            if not id_logging_done:
                                logging.info('Updating post_id: %s' % post_id)
                                id_logging_done = True

                            logging.info('new %s: %s' % ('x', new_feature['geometry']['x']))
                            logging.info('exist %s: %s' % ('x', round(existing_feature_info['SHAPE']['x'], 2)))

                        if round(existing_feature_info['SHAPE']['y'], 2) != new_feature['geometry']['y']:
                            modification_list['y'] = new_feature['geometry']['y']

                            if not id_logging_done:
                                logging.info('Updating post_id: %s' % post_id)
                                id_logging_done = True

                            logging.info('new %s: %s' % ('y', new_feature['geometry']['y']))
                            logging.info('exist %s: %s' % ('y', round(existing_feature_info['SHAPE']['y'], 2)))

            if len(modification_list) > 0:
                where_statement = 'post_id=\'' + post_id + '\''
                query_result = online_content_flayer.query(where=where_statement)
                existing_feature = query_result.features[0]

                edited_feature = existing_feature

                for attrib in modification_list:
                    if attrib == 'x':
                        edited_feature.geometry['x'] = modification_list['x']
                    elif attrib == 'y':
                        edited_feature.geometry['y'] = modification_list['y']
                    else:
                        edited_feature.attributes[attrib] = modification_list[attrib]

                # Update existing feature
                try:
                    result = online_content_flayer.edit_features(updates=[edited_feature])
                    counter_updated = counter_updated + 1
                    logging.info('Feature updated', result)
                except TimeoutError as error:
                    logging.error('Time out error on updating feature', error)
                    print('Time out error on updating feature', error)
            else:
                counter_unchanged = counter_unchanged + 1
                # logging.info('Feature already exists with same information. No action.')

        else:
            # Add feature to feature layer
            try:
                result = online_content_flayer.edit_features(adds=[new_feature])
                logging.info('Adding post_id: %s' % post_id)
                logging.info('Feature added to the feature layer.', result)
                counter_added = counter_added + 1
            except TimeoutError as error:
                logging.error('Time out error on adding feature', error)

        counter_processed = counter_processed + 1

    logging.info('Processed: %s, Added: %s, Updated: %s, Unchanged: %s'
                 % (counter_processed, counter_added, counter_updated, counter_unchanged))
    print('Processed: %s, Added: %s, Updated: %s, Unchanged: %s'
          % (counter_processed, counter_added, counter_updated, counter_unchanged))

    logging.info('##### END OF PROCESS')


if __name__ == '__main__':

    LOGGING_FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.INFO, filename='logs.txt', filemode='a',
                        datefmt='%Y%m%d %H:%M:%S', format=LOGGING_FORMAT)
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    main()
