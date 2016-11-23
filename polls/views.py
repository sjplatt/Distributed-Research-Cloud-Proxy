from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from django.db import connection
import redis
import json
import dateutil.parser
import datetime
import requests

# Create your views here.

cacheQuery = redis.StrictRedis(host='localhost', port=6379, db=0)
cacheSource = redis.StrictRedis(host='localhost', port=6379, db=1)

static_callback = "http://128.2.213.140"

def customDeserializeDatetime(obj):
    try:
        if "datetime" in str(obj):
            return dateutil.parser.parse(obj.split(":")[1])
        else:
            return obj
    except:
        return obj

def customSerializeDatetime(obj):
    if isinstance(obj, datetime.datetime):
        return "datetime:" + obj.isoformat()
    raise TypeError("Type not serializable Boo")

 # Returns the datetime parameter from params

def removeDateTime(params):
    if params is None:
        return None
    else:
        res = []
        for temp in params:
            if not isinstance(temp, datetime.datetime):
                res += [temp]

        return tuple(res)

def sendCallBack(key, curSource):
    sources = eval(cacheSource.get(removeDateTimeFromKey(key)))
    for source in sources:
        print "SENDING CALLBACK TO " + str(source)
        r = requests.post("http://128.2.213.140:5555/proxy/callBack/", data=removeDateTimeFromKey(key))

def convertToKey(sql, params):
    if params is None:
        return str(sql)
    elif not "datetime" in str(params):
        return str(sql) + "|" + str(params)
    else:
        return str(sql) + "|" + json.dumps(params, default=customSerializeDatetime)

def removeDateTimeFromKey(key):
    if not "datetime" in str(key):
        return key

    body = key.split("|")
    query, params = body[0], None
    if len(body) == 2:
        params = json.loads(body[1], object_pairs_hook=customDeserializeDatetime)
        params = [p for p in params if not "datetime" in str(p)]

    return convertToKey(query, params)

def inCache(sql, params):
    key = convertToKey(sql, params)
    if not "datetime" in str(key):
        return cacheQuery.get(key), cacheSource.get(key)
    
    no_datetime = removeDateTimeFromKey(key)
    for k in cacheQuery.keys():
        if removeDateTimeFromKey(k) == no_datetime:
            return cacheQuery.get(k), cacheSource.get(k)
    return None, None

def convertToValue(data, rc, error, ret):
    return json.dumps([ret, data, rc, error], default=customSerializeDatetime)

def lookupAndCompare(key, curSource):
    split = key.split("|")
    sql, params = split[0], None
    if len(split) == 2:
        if "datetime" in str(split[1]):
            params = json.loads(split[1], object_pairs_hook=customDeserializeDatetime)
        else:
            params = eval(split[1])
        if not params is None:
            params = [customDeserializeDatetime(p) for p in params]

    curValue = cacheQuery.get(key)
    
    if not curValue:
        return
    
    with connection.cursor() as cursor:
        # 1) Execute query
        ret = cursor.execute(sql, params)

        rc = cursor.rowcount
        error = False
        val = []
        
        try:
            val = cursor.fetchall()
            error = False
        except:
            val = []
            error = True

        # Current value for query
        myValue = convertToValue(val, rc, error, ret)
        if not myValue == curValue:
            putInCache(key, myValue, curSource)
            sendCallBack(key, curSource)
    
def containsDateTime(data):
    return "datetime.datetime" in str(data)

# Puts values into the cache
def putInCache(key, value, curSource):
    if not "SELECT" in str(key):
        return

    cacheQuery.set(key, value)

    key = removeDateTimeFromKey(key)
    sources = cacheSource.get(key)
    if not sources:
        cacheSource.set(key, str([curSource]))
    else:
        sources = eval(sources)
        if not curSource in sources:
            sources += [curSource]
            cacheSource.set(key, str(sources))

@csrf_exempt
def index(request):
    # cacheQuery.flushall()
    # cacheSource.flushall()
    body = request.body.split("|")
    source = request.META["HTTP_SOURCE"]
    query, params = body[0], None

    if len(body) == 2:
        params = json.loads(body[1], object_pairs_hook=customDeserializeDatetime)
        if not params is None:
            params = [customDeserializeDatetime(p) for p in params]

    with connection.cursor() as cursor:
        # Execute query
        ret = cursor.execute(query, params)

        rc = cursor.rowcount
        error = False
        val = []
        
        try:
            val = cursor.fetchall()
            error = False
        except:
            val = []
            error = True

        # Add key to cache
        if "SELECT" in str(query):
            putInCache(convertToKey(query, params), convertToValue(val,rc,error,ret), source)
        
        if not "SELECT" in str(query):
            # Check all existing keys for callbacks
            for key in cacheQuery.keys():
                if not key == convertToKey(query, params):
                    lookupAndCompare(key, source)

    return HttpResponse(convertToValue(val,rc,error, ret))