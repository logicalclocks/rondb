# Feature Store REST API Server

This API server allows users to retrieve single/batch feature vectors from a feature view.

## Single feature vector

### Request

`POST /{api-version}/feature_store`

**Body**

```
{
        "featureStoreName": "fsdb002",
        "featureViewName": "sample_2",
        "featureViewVersion": 1,
        "passedFeatures": {},
        "entries": {
                "id1": 36
        },
        "metadataOptions": {
                "featureName": true,
                "featureType": true
        },
        "options": {
                "validatePassedFeatures": true,
                "includeDetailedStatus": true
        }
}
```

**Parameters**

**parameter**      | **type**    | **note**
------------------ | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
featureStoreName   | string      |
featureViewName    | string      |
featureViewVersion | number(int) |
entries            | objects     | Map of serving key of feature view as key and value of serving key as value. Serving key are a set of the primary key of feature groups which are included in the feature view query. If feature groups are joint with prefix, the primary key needs to be attached with prefix.
passedFeatures     | objects     | Optional. Map of feature name as key and feature value as value. This overwrites feature values in the response. You can choose whether or not to validate the passedFeatures keys and types by setting the `validatePassedFeatures` option to true. 
metadataOptions    | objects     | Optional. Map of metadataoption as key and boolean as value. Default metadata option is false. Metadata is returned on request. Metadata options available: 1\. featureName 2\. featureType  |
options            | objects     | Optional. Map of option as key and boolean as value. Default option is false. Options available: 1\. validatePassedFeatures 2\. includeDetailedStatus

### Response

```
{
        "features": [
                36,
                "2022-01-24",
                "int24",
                "str14"
        ],
        "metadata": [
                {
                        "featureName": "id1",
                        "featureType": "bigint"
                },
                {
                        "featureName": "ts",
                        "featureType": "date"
                },
                {
                        "featureName": "data1",
                        "featureType": "string"
                },
                {
                        "featureName": "data2",
                        "featureType": "string"
                }
        ],
        "status": "COMPLETE",
        "detailedStatus": [
                {
                        "featureGroupId": 1,
                        "httpStatus": 200,
                },
                {
                        "featureGroupId": 2,
                        "httpStatus": 200,
                },
        ]
}
```

### Error handling

**Code** | **reason**                            | **response**
-------- | ------------------------------------- | ------------------------------------
200      |                                       |
400      | Requested metadata does not exist     |
400      | Error in pk or passed feature value   |
404      | Missing row corresponding to pk value |
401      | Access denied                         | Access unshared feature store failed
500      | Failed to read feature store metadata |

**Response with pk/pass feature error**

```
{
        "code": 12,
        "message": "Wrong primay-key column. Column: ts",
        "reason": "Incorrect primary key."
}
```

**Response with metadata error**

```
{
        "code": 2,
        "message": "",
        "reason": "Feature store does not exist."
}
```

**Pk value no match**

```
{
        "features": [
                9876543,
                null,
                null,
                null
        ],
        "metadata": null,
        "status": "MISSING"
}
```

**Detailed Status**

If `includeDetailedStatus` option is set to true, detailed status is returned in the response. Detailed status is a list of feature group id and http status code, corresponding to each read operations perform internally by RonDB. Meaning is as follows:

- `featureGroupId`: Id of the feature group, used to identify which table the operation correspond from.
- `httpStatus`: Http status code of the operation. 
        * 200 means success
        * 400 means bad request, likely pk name is wrong or pk is incomplete. In particular, if pk for this table/feature group is not provided in the request, this http status is returned.
        * 404 means no row corresponding to PK
        * 500 means internal error.

Both `404` and `400` set the status to `MISSING` in the response. Examples below corresponds respectively to missing row and bad request.


Missing Row: The pk name,value was correctly passed but the corresponding row was not found in the feature group.
```
{
        "features": [
                36,
                "2022-01-24",
                null,
                null
        ],
        "status": "MISSING",
        "detailedStatus": [
                {
                        "featureGroupId": 1,
                        "httpStatus": 200,
                },
                {
                        "featureGroupId": 2,
                        "httpStatus": 404,
                },
        ]
}
```

Bad Request e.g pk name,value pair for FG2 not provided or the corresponding column names was incorrect.
```
{
        "features": [
                36,
                "2022-01-24",
                null,
                null
        ],
        "status": "MISSING",
        "detailedStatus": [
                {
                        "featureGroupId": 1,
                        "httpStatus": 200,
                },
                {
                        "featureGroupId": 2,
                        "httpStatus": 400,
                },
        ]
}
```


## Batch feature vectors

### Request

`POST /{api-version}/batch_feature_store`

**Body**

```
{
        "featureStoreName": "fsdb002",
        "featureViewName": "sample_2",
        "featureViewVersion": 1,
        "passedFeatures": [],
        "entries": [
                {
                        "id1": 16
                },
                {
                        "id1": 36
                },
                {
                        "id1": 71
                },
                {
                        "id1": 48
                },
                {
                        "id1": 29
                }
        ],
        "requestId": null,
        "metadataOptions": {
                "featureName": true,
                "featureType": true
        },
        "options": {
                "validatePassedFeatures": true,
                "includeDetailedStatus": true
        }
}
```

**Parameters**

**parameter**      | **type**         | **note**
------------------ | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
featureStoreName   | string           |
featureViewName    | string           |
featureViewVersion | number(int)      |
entries            | `array<objects>` | Each items is a map of serving key as key and value of serving key as value. Serving key of feature view.
passedFeatures     | `array<objects>` | Optional. Each items is a map of feature name as key and feature value as value. This overwrites feature values in the response. If provided, its size and order has to be equal to the size of entries. Item can be null.
metadataOptions    | objects          | Optional. Map of metadataoption as key and boolean as value. Default metadata option is false. Metadata is returned on request. Metadata options available: 1\. featureName 2\. featureType
options            | objects          | Optional. Map of option as key and boolean as value. Default option is false. Options available: 1\. validatePassedFeatures 2\. includeDetailedStatus

### Response

```
{
        "features": [
                [
                        16,
                        "2022-01-27",
                        "int31",
                        "str24"
                ],
                [
                        36,
                        "2022-01-24",
                        "int24",
                        "str14"
                ],
                [
                        71,
                        null,
                        null,
                        null
                ],
                [
                        48,
                        "2022-01-26",
                        "int92",
                        "str31"
                ],
                [
                        29,
                        "2022-01-03",
                        "int53",
                        "str91"
                ]
        ],
        "metadata": [
                {
                        "featureName": "id1",
                        "featureType": "bigint"
                },
                {
                        "featureName": "ts",
                        "featureType": "date"
                },
                {
                        "featureName": "data1",
                        "featureType": "string"
                },
                {
                        "featureName": "data2",
                        "featureType": "string"
                }
        ],
        "status": [
                "COMPLETE",
                "COMPLETE",
                "MISSING",
                "COMPLETE",
                "COMPLETE"
        ],
        "detailedStatus": [
                [{
                        "featureGroupId": 1,
                        "httpStatus": 200,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 200,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 404,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 200,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 200,
                }]
        ]
}
```

note: Order of the returned features are the same as the order of entries in the request.

### Error handling

**Code** | **reason**                            | **response**
-------- | ------------------------------------- | ------------------------------------
200      |                                       |
400      | Bad Request                           |
404      | Missing row corresponding to pk value |
401      | Access denied                         | Access unshared feature store failed
500      | Failed to read feature store metadata |

**Response with partial failure**

```
{
        "features": [
                [
                        81,
                        "id81",
                        "2022-01-29 00:00:00",
                        6
                ],
                null,
                [
                        51,
                        null,
                        null,
                        null,
                ]
        ],
        "metadata": null,
        "status": [
                "COMPLETE",
                "ERROR",
                "MISSING"
        ],
        "detailedStatus": [
                [{
                        "featureGroupId": 1,
                        "httpStatus": 200,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 400,
                }],
                [{
                        "featureGroupId": 1,
                        "httpStatus": 404,
                }]
        ]
}
```

## Access control to feature store

Authentication to feature store is done by hopsworks API key, set the X-API-KEY header in the request to authenticate. If the API key is not set or invalid, the response will be 401 Unauthorized.
