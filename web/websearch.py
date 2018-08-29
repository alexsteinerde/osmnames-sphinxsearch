#!/usr/bin/env python
# -*- coding: utf-8 -*-
# WebSearch gate for OSMNames-SphinxSearch
#
# Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
#   All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential
# Author: Martin Mikita (martin.mikita @ klokantech.com)
# Date: 15.07.2016

from flask import Flask, request, Response, render_template, url_for, redirect
from pprint import pprint, PrettyPrinter
from json import dumps
from os import getenv, path, utime
from time import time, mktime
from datetime import datetime
import sys
import MySQLdb
import re
import natsort
import rfc822   # Used for parsing RFC822 into datetime
import email    # Used for formatting TS into RFC822
import traceback


# Prepare global variables
SEARCH_MAX_COUNT = 100
SEARCH_DEFAULT_COUNT = 20
if getenv('SEARCH_MAX_COUNT'):
    SEARCH_MAX_COUNT = int(getenv('SEARCH_MAX_COUNT'))
if getenv('SEARCH_DEFAULT_COUNT'):
    SEARCH_DEFAULT_COUNT = int(getenv('SEARCH_DEFAULT_COUNT'))

TMPFILE_DATA_TIMESTAMP = "/tmp/osmnames-sphinxsearch-data.timestamp"

NOCACHEREDIRECT = False
if getenv('NOCACHEREDIRECT'):
    NOCACHEREDIRECT = getenv('NOCACHEREDIRECT')

# Prepare global variable for Last-modified Header
try:
    mtime = path.getmtime(TMPFILE_DATA_TIMESTAMP)
except OSError:
    with open(TMPFILE_DATA_TIMESTAMP, 'a'):
        utime(TMPFILE_DATA_TIMESTAMP, None)
    mtime = time()
DATA_LAST_MODIFIED = email.utils.formatdate(mtime, usegmt=True)

# Filter attributes values
# dict[ attribute ] = list(values)
CHECK_ATTR_FILTER = ['country_code', 'class']
ATTR_VALUES = {}


app = Flask(__name__, template_folder='templates/')
app.debug = not (getenv('WEBSEARCH_DEBUG') is None)


# ---------------------------------------------------------
def get_db_cursor():
    # connect to the mysql server
    # default server configuration
    host = '127.0.0.1'
    port = 9306
    if getenv('WEBSEARCH_SERVER'):
        host = getenv('WEBSEARCH_SERVER')
    if getenv('WEBSEARCH_SERVER_PORT'):
        port = int(getenv('WEBSEARCH_SERVER_PORT'))

    db = MySQLdb.connect(host=host, port=port, user='root')
    cursor = db.cursor()
    return db, cursor


def get_query_result(cursor, sql, args):
    """
    Get result from SQL Query.

    Boolean, {'matches': [{'weight': 0, 'id', 'attrs': {}}], 'total_found': 0}
    """
    status = False
    result = {
        'matches': [],
        'status': False,
        'total_found': 0,
    }
    try:
        q = cursor.execute(sql, args)  # noqa
        # pprint([sql, args, cursor._last_executed, q])
        desc = cursor.description
        matches = []
        status = True
        for row in cursor:
            match = {
                'weight': 0,
                'attrs': {},
                'id': 0,
            }
            for (name, value) in zip(desc, row):
                col = name[0]
                if col == 'id':
                    match['id'] = value
                elif col == 'weight':
                    match['weight'] = value
                else:
                    match['attrs'][col] = value
            matches.append(match)
        # ~ for row in cursor
        result['matches'] = matches

        cursor.execute('SHOW META LIKE %s', ('total_found',))
        for row in cursor:
            result['total_found'] = int(row[1])
    except Exception as ex:
        result['message'] = str(ex)

    result['status'] = status
    return status, result


# ---------------------------------------------------------
def get_attributes_values(index, attributes):
    """
    Get attributes distinct values, using data from index.

    dict[ attribute ] = list(values)
    """
    global ATTR_VALUES

    try:
        db, cursor = get_db_cursor()
    except Exception as ex:
        print(str(ex))
        return False

    # Loop over attributes
    if isinstance(attributes, str):
        attributes = [attributes, ]

    for attr in attributes:
        # clear values
        ATTR_VALUES[attr] = []
        count = 200
        total_found = 0
        # get attributes values for index
        sql_query = 'SELECT {} FROM {} GROUP BY {} LIMIT {}, {}'
        sql_meta = 'SHOW META LIKE %s'
        found = 0
        try:
            while total_found == 0 or found < total_found:
                cursor.execute(sql_query.format(attr, index, attr, found, count), ())
                for row in cursor:
                    found += 1
                    ATTR_VALUES[attr].append(str(row[0]))
                if total_found == 0:
                    cursor.execute(sql_meta, ('total_found',))
                    for row in cursor:
                        total_found = int(row[1])
                        # Skip this attribute, if total found is more than max_matches
                        if total_found > 1000:
                            del(ATTR_VALUES[attr])
                            found = total_found
            if found == 0:
                del(ATTR_VALUES[attr])
        except Exception as ex:
            db.close()
            print(str(ex))
            return False

    db.close()
    return True


# ---------------------------------------------------------
def mergeResultObject(result_old, result_new):
    """
    Merge two result objects into one.

    Order matches by weight
    """
    # Merge matches
    weight_matches = {}
    unique_id = 0
    unique_ids_list = []

    for matches in [result_old['matches'], result_new['matches'], ]:
        for row in matches:
            if row['id'] in unique_ids_list:
                result_old['total_found'] -= 1  # Decrease total found number
                continue
            unique_ids_list.append(row['id'])
            weight = str(row['weight'])
            if weight in weight_matches:
                weight += '_{}'.format(unique_id)
                unique_id += 1
            weight_matches[weight] = row

    # Sort matches according to the weight and unique id
    sorted_matches = natsort.natsorted(weight_matches.items(), reverse=True)
    matches = []
    i = 0
    for row in sorted_matches:
        matches.append(row[1])
        i += 1
        # Append only first #count rows
        if 'count' in result_old and i >= result_old['count']:
            break

    result = result_old.copy()
    result['matches'] = matches
    result['total_found'] += result_new['total_found']
    if 'message' in result_new and result_new['message']:
        result['message'] = ', '.join(result['message'], result_new['message'])

    return result


# ---------------------------------------------------------
def prepareResultJson(result):
    """Prepare JSON from pure Result array from SphinxQL."""
    if 'start_index' not in result:
        result = {
            'start_index': 0,
            'count': 0,
            'total_found': 0,
            'matches': [],
        }

    response = {
        'results': [],
        'startIndex': result['start_index'],
        'count': result['count'],
        'totalResults': result['total_found'],
    }
    if 'message' in result and result['message']:
        response['message'] = result['message']

    for row in result['matches']:
        r = row['attrs']
        res = {'rank': row['weight'], 'id': row['id']}
        for attr in r:
            if isinstance(r[attr], str):
                try:
                    res[attr] = r[attr].decode('utf-8')
                except:
                    res[attr] = r[attr]
            else:
                res[attr] = r[attr]
        # Prepare bounding box from West/South/East/North attributes
        if 'west' in res:
            res['boundingbox'] = [res['west'], res['south'], res['east'], res['north']]
            del res['west']
            del res['south']
            del res['east']
            del res['north']
        # Empty values for KlokanTech NominatimMatcher JS
        # res['address'] = {
        #     'country_code': '',
        #     'country': '',
        #     'city': None,
        #     'town': None,
        #     'village': None,
        #     'hamlet': rr['name'],
        #     'suburb': '',
        #     'pedestrian': '',
        #     'house_number': '1'
        # }
        response['results'].append(res)

    # Prepare next and previous index
    next_index = result['start_index'] + result['count']
    if next_index <= result['total_found']:
        response['nextIndex'] = next_index
    prev_index = result['start_index'] - result['count']
    if prev_index >= 0:
        response['previousIndex'] = prev_index

    response['results'] = prepareNameSuffix(response['results'])

    return response


# ---------------------------------------------------------
def parseDisplayName(row):
    # commas = row['display_name'].count(',')
    parts = row['display_name'].split(', ')
    newrow = {}
    if len(parts) == 5:
        newrow['city'] = parts[1]
        newrow['state'] = parts[3]
        newrow['country'] = parts[4]
    if len(parts) == 6:
        newrow['city'] = parts[1]
        newrow['state'] = parts[4]
        newrow['county'] = parts[4]
        newrow['country'] = parts[5]

    for field in newrow:
        if field not in row:
            row[field] = newrow[field]
        if not row[field]:
            row[field] = newrow[field]

    return row


def prepareNameSuffix(results):
    """Parse and prepare name_suffix based on results."""
    counts = {'country_code': [], 'city': [], 'name': [], 'county': []}

    # Separate different country codes
    for row in results:
        for field in counts.keys():
            if field not in row:
                continue
            if row[field] in counts[field]:
                continue
            # Skip states for not-US
            if 'country_code' in row and row['country_code'] != 'us' and field == 'state':
                continue
            counts[field].append(row[field])

    # Prepare name suffix based on counts
    newresults = []
    for row in results:
        try:
            if not row['city']:
                row = parseDisplayName(row)

            name_suffix = []
            if (row['type'] != 'city' and len(row['city']) > 0 and row['name'] != row['city'] and
               (len(counts['city']) > 1 or len(counts['name']) > 1)):
                name_suffix.append(row['city'])
            if row['country_code'] == 'us' and len(counts['state']) > 1 and len(row['state']) > 0:
                name_suffix.append(row['state'])
            if len(counts['county']) > 1:
                name_suffix.append(row['county'])
            if len(counts['country_code']) > 1:
                name_suffix.append(row['country_code'].upper())
            row['name_suffix'] = ', '.join(name_suffix)
        except:
            pass
        newresults.append(row)

    return newresults


# ---------------------------------------------------------
def formatResponse(data, code=200):
    """Format response output."""
    # Format json - return empty
    result = data['result'] if 'result' in data else {}
    if app.debug and 'debug' in data:
        result['debug'] = data['debug']
    output_format = 'json'
    if request.args.get('format'):
        output_format = request.args.get('format')
    if 'format' in data:
        output_format = data['format']

    tpl = data['template'] if 'template' in data else 'answer.html'
    if output_format == 'html' and tpl is not None:
        if 'route' not in data:
            data['route'] = '/'
        return render_template(tpl, rc=(code == 200), **data), code

    json = dumps(result)
    mime = 'application/json'
    # Append callback for JavaScript
    if request.args.get('json_callback'):
        json = "{}({});".format(
            request.args.get('json_callback'),
            json)
        mime = 'application/javascript'
    if request.args.get('callback'):
        json = "{}({});".format(
            request.args.get('callback'),
            json)
        mime = 'application/javascript'
    resp = Response(json, mimetype=mime)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    # Cache results for 4 hours in Web Browsers and 12 hours in CDN caches
    resp.headers['Cache-Control'] = 'public, max-age=14400, s-maxage=43200'
    resp.headers['Last-Modified'] = DATA_LAST_MODIFIED
    return resp, code

class MyPrettyPrinter(PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, unicode):
            return ('"' + object.encode('utf-8') + '"', True, False)
        return PrettyPrinter.format(self, object, context, maxlevels, level)


# Custom template filter - nl2br
@app.template_filter()
def nl2br(value):
    if isinstance(value, dict):
        for key in value:
            value[key] = nl2br(value[key])
        return value
    elif isinstance(value, str):
        return value.replace('\n', '<br>')
    else:
        return value


# Custom template filter - ppretty
@app.template_filter()
def ppretty(value):
    return MyPrettyPrinter().pformat(value).decode('utf-8')


# =============================================================================
"""
Reverse geo-coding support

Author: Komodo Solutions
        enquiries@komodo-solutions.co.uk
        http://www.komodo-solutions.co.uk
Date:   11.07.2017
"""


# reverse_search - find the closest place in the data set to the supplied coordinates
# lon     - float   - the longitude coordinate, in degrees, for the closest place match
# lat     - float   - the latitude coordinate, in degrees, for the closest place match
# classes - array   - the array of classes to filter, empty array without filtering
# debug   - boolean - if true, include diagnostics in the result
# returns - result, distance tuple
def reverse_search(lon, lat, classes, debug):
    result = {
        'total_found': 0,
        'count': 0,
        'matches': []
    }

    if debug:
        result['debug'] = {
            'longitude': lon,
            'latitude': lat,
            'queries': [],
            'results': [],
        }

    try:
        db, cursor = get_db_cursor()
    except Exception as ex:
        status = False
        result['message'] = str(ex)
        result['status'] = status
        return result, 0

    # We attempt to find rows using a small bounding box to
    # limit the impact of the distance calculation.
    # If no rows are found with the current bounding box
    # we double it and try again, until a result is returned.

    delta = 0.0004
    count = 0

    while count == 0:
        delta *= 2
        lon_min = lon - delta
        lon_max = lon + delta
        lat_min = lat - delta
        lat_max = lat + delta

        # Bound the latitude
        lat_min = max(min(lat_min, 90.0), -90.0)
        lat_max = max(min(lat_max, 90.0), -90.0)
        # we use the built-in GEODIST function to calculate distance
        select = ("SELECT *, GEODIST(" + str(lat) + ", " + str(lon) +
                  ", lat, lon, {in=degrees, out=meters}) as distance"
                  " FROM ind_name_exact WHERE ")

        """
        SphinxQL does not support the OR operator or the NOT BETWEEN syntax so the only
        viable approach is to use 2 queries with different longitude conditions for
        180 meridan spanning cases
        """
        wherelon = []
        if (lon_min < -180.0):
            wherelon.append("lon BETWEEN {} AND 180.0".format(360.0 + lon_min))
            wherelon.append("lon BETWEEN -180.0 AND {}".format(lon_max))
        elif (lon_max > 180.0):
            wherelon.append("lon BETWEEN {} AND 180.0".format(lon_min))
            wherelon.append("lon BETWEEN -180.0 AND {}".format(-360.0 + lon_max))
        else:
            wherelon.append("lon BETWEEN {} AND {}".format(lon_min, lon_max))
        # latitude condition is the same for all cases
        wherelat = "lat BETWEEN {} AND {}".format(lat_min, lat_max)
        # limit the result set to the single closest match
        limit = " ORDER BY distance ASC LIMIT 1"

        myresult = {}
        if not classes:
            classes = [""]
        # form the final queries and execute
        for where in wherelon:
            for cl in classes:
                sql = select + " AND ".join([where, wherelat])
                if cl:
                    sql += " AND class='{}' ".format(cl)
                sql += limit
                # Boolean, {'matches': [{'weight': 0, 'id', 'attrs': {}}], 'total_found': 0}
                status, result_new = get_query_result(cursor, sql, ())
                if debug:
                    result['debug']['queries'].append(sql)
                    result['debug']['results'].append(result_new)
                if 'matches' in myresult and len(myresult['matches']) > 0:
                    myresult = mergeResultObject(myresult, result_new)
                else:
                    myresult = result_new.copy()

        count = len(myresult['matches'])
    db.close()

    if debug:
        result['debug']['matches'] = myresult['matches']

    smallest_row = None
    smallest_distance = None

    # For the rows returned, find the smallest calculated distance
    # (the 180 meridian case may result in 2 rows to check)
    for match in myresult['matches']:
        distance = match['attrs']['distance']

        if smallest_row is None or distance < smallest_distance:
            smallest_row = match
            smallest_distance = distance

    result = mergeResultObject(result, myresult)
    result['count'] = 1
    result['matches'] = [smallest_row]
    result['start_index'] = 1
    result['status'] = True
    result['total_found'] = 1
    return result, smallest_distance


# ---------------------------------------------------------
@app.route('/r/<lon>/<lat>.js', defaults={'classes': None})
@app.route('/r/<classes>/<lon>/<lat>.js')
def reverse_search_url(lon, lat, classes):
    """REST API for reverse_search."""
    code = 400
    data = {'format': 'json'}

    debug = request.args.get('debug')
    times = {}

    try:
        if debug:
            times['start'] = time()

        try:
            lon = float(lon)
            lat = float(lat)
        except:
            data['result'] = {'message': 'Longitude and latitude must be numeric.'}
            return formatResponse(data, code)

        if lon < -180.0 or lon > 180.0:
            data['result'] = {'message': 'Invalid longitude.'}
            return formatResponse(data, code)
        if lat < -90.0 or lat > 90.0:
            data['result'] = {'message': 'Invalid latitude.'}
            return formatResponse(data, code)

        if debug:
            times['prepare'] = time() - times['start']

        code = 200
        filter_classes = []
        if classes:
            # This argument can be list separated by comma
            filter_classes = classes.encode('utf-8').split(',')
        result, distance = reverse_search(lon, lat, filter_classes, debug)
        data['result'] = prepareResultJson(result)
        if debug:
            times['process'] = time() - times['start']
            data['debug'] = result['debug']
            data['debug']['distance'] = distance
            data['debug_times'] = times
    except:
        traceback.print_exc()
        data['result'] = {'message': 'Unexpected failure to handle this request. Please, contact sysadmin.'}
        code = 500

    return formatResponse(data, code)


@app.route('/r/<lon>/<lat>', defaults={'classes': None})
@app.route('/r/<classes>/<lon>/<lat>')
def reverse_search_url_public(lon, lat, classes):
    if NOCACHEREDIRECT:
        return redirect(NOCACHEREDIRECT, code=302)

    return reverse_search_url(lon, lat, classes)

# =============================================================================
# End Reverse geo-coding support
# =============================================================================


# Load attributes at runtime
get_attributes_values('ind_name_exact', CHECK_ATTR_FILTER)
pprint(ATTR_VALUES)


"""
Main launcher
"""
if __name__ == '__main__':
    app.run(threaded=False, host='0.0.0.0', port=8000)
