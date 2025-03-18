def makeRollup(parentMap, target, groupFields, calFields):
    result = []
    groupField = groupFields[0]
    groupFieldName = groupField
    
    try:
        keywwordIndex = groupFieldName.index('keyword')
        groupFieldName = groupFieldName[0:keywwordIndex-1]
    except:
        print('No keyword in the group name!')
        
    aggGroup = target['group_by_' + groupFieldName]
    buckets = aggGroup['buckets']
    if len(groupFields) == 1:
        for bucket in buckets:
            bucketMap = {}
            if not parentMap is None:
                for k, v in parentMap.items():
                    bucketMap[k] = v
            bucketMap[groupFieldName] = bucket['key']
            for calField in calFields:
                bucketMap[calField] = bucket[calField]['value']
            result.append(bucketMap)
    else:
        for bucket in buckets:
            if parentMap is None:
                parentMap = {}
            parentMap[groupFieldName] =  bucket['key']
            result.extend(makeRollup(parentMap, bucket, groupFields[1:], calFields))
    return result
"""
# test
from elasticsearch import Elasticsearch
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
ssl.CERT_None = True

def queryData(dbIp, dbUser, dbPwd, startDate, endDate):
    query = {
      "size": 0,
      "query": {
        "bool": {
          "filter": {
            "range": {
              "CREATE_DATE": {
                "gte": startDate,
                "lte": endDate
              }
            }
          }
        }
      },
      "aggs": {
        "group_by_CREATE_DATE": {
          "terms": {
            "field": "CREATE_DATE",
            "order": {
              "_term": "asc"
            }
          },
          "aggs": {
            "group_by_groupName": {
              "terms": {
                "field": "groupName.keyword"
              },
              "aggs": {
                "ONLINE_FLOWIN": {
                  "sum": {
                    "field": "ONLINE_FLOWIN"
                  }
                },
                "OFFLINE_FLOWIN": {
                  "sum": {
                    "field": "OFFLINE_FLOWIN"
                  }
                },
                "HIT_FLOWOUT": {
                  "sum": {
                    "field": "HIT_FLOWOUT"
                  }
                },
                "MISS_FLOWOUT": {
                  "sum": {
                    "field": "MISS_FLOWOUT"
                  }
                }
              }
            }
          }
        }
      }
    }
    es = Elasticsearch('https://ip:9200', http_auth=(dbUser, dbPwd), use_ssl=False, verify_certs=False, ssl_show_warn=False)
    result = es.search(index="TA-MIN-CDN_FLUX", doc_type="esType", body=query)
    
    if result is None or result['aggregations'] is None:
        return []
    else:
        retData = makeRollup(None, result['aggregations'], ['CREATE_DATE', 'groupName.keyword'], ['ONLINE_FLOWIN', 'OFFLINE_FLOWIN', 'HIT_FLOWOUT', 'MISS_FLOWOUT'])
        return retData
"""