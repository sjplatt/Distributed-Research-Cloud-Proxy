from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from django.db import connection
import redis
import json
import dateutil.parser
import datetime

# Create your views here.

cacheQuery = redis.StrictRedis(host='localhost', port=6379, db=0)
cacheSource = redis.StrictRedis(host='localhost', port=6379, db=1)


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
    sources = list(cacheSource.get(key))
    for source in sources:
        if not source == curSource:
            print "SENDING CALLBACK TO " + str(source)

def convertToKey(sql, params):
    if params is None:
        return str(sql)
    else:
        return str(sql) + "|" + str(removeDateTime(params))

def convertToValue(data, rc, error, ret):
    return json.dumps([ret, data, rc, error], default=customSerializeDatetime)

def lookupAndCompare(key, params, curSource):
    split = key.split("|")
    sql, params = split[0], None
    if len(split) == 2:
        params = list(split[1])

    curValue = cacheQuery.get(convertToKey(sql,params))
    
    if not curValue:
        return
    
    with connection.cursor() as cursor:
        # 1) Execute query
        cursor.execute(sql, params)

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
        myValue = convertToValue(val, rc, error)
        if not myValue == curValue:
            putInCache(key, myValue)
            sendCallBack(key, curSource)
            print("Changed Query")
        else:
            print("Non-changed Query")
    
def containsDateTime(data):
    return "datetime.datetime" in str(data)

# Puts values into the cache
def putInCache(key, value, curSource):
    if not "SELECT" in str(key):
        return

    cacheQuery.set(key, value)

    sources = cacheSource.get(key)
    if not sources:
        cacheSource.set(key, [curSource])
    else:
        if not curSource in sources:
            sources += [curSource]
            cacheSource.set(key, sources)

@csrf_exempt
def index(request):
    body = request.body.split("|")
    source = request.META["HTTP_SOURCE"]
    query, params = body[0], None
    #print(query, params)
    if len(body) == 2:
        params = json.loads(body[1], object_pairs_hook=customDeserializeDatetime)
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

        # If Key in cache, check callbacks for key
        if not "SELECT" in str(query):
            lookupAndCompare(convertToKey(query, params), source)

        # Add key to cache
        if "SELECT" in str(query):
            putInCache(convertToKey(query, params), convertToValue(val,rc,error,ret), source)
        
        if not "SELECT" in str(query):
            # Check all existing keys for callbacks
            for key in cacheQuery.keys():
                if not key == convertToKey(query, params):
                    lookupAndCompare(key, source)

    return HttpResponse(convertToValue(val,rc,error, ret))