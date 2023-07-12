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
passedFeatures     | objects     | Optional. Map of feature name as key and feature value as value. This overwrites feature values in the response.
metadataOptions    | objects     | Optional. Map of metadataoption as key and boolean as value. Default metadata option is false. Metadata is returned on request. Metadata options available: 1\. featureName 2\. featureType  |

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
        "status": "COMPLETE"
}
```

### Error handling

**Code** | **reason**                            | **response**
-------- | ------------------------------------- | ------------------------------------
200      |                                       |
400      | Requested metadata does not exist     |
400      | Error in pk or passed feature value   |
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
                        "2022-01-22",
                        "int3",
                        "str97"
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
                "COMPLETE",
                "COMPLETE",
                "COMPLETE"
        ]
}
```

note: Order of the returned features are the same as the order of entries in the request.

### Error handling

**Code** | **reason**                            | **response**
-------- | ------------------------------------- | ------------------------------------
200      |                                       |
400      | Requested metadata does not exist     |
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
                        "id51",
                        "2022-01-10 00:00:00",
                        49
                ]
        ],
        "metadata": null,
        "status": [
                "COMPLETE",
                "ERROR",
                "COMPLETE"
        ]
}
```

## Access control to feature store

Authentication to feature store is done by hopsworks API key.
