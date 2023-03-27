# RonDB REST API Server 

Currently, the REST API server only supports batched and non-batched  primary key operations. Default mappings of MySQL data types to JSON data types are as follows:


| MySQL Data Type                          | JSON Data Type        |
| ---------------------------------------- | --------------------- |
| TINYINT, SMALLINT MEDIUMINT, INT, BIGINT | number                |
| FLOAT, DOUBLE, DECIMAL                   | number                |
| CHAR, VARCHAR                            | escaped string        |
| BINARY, VARBINARY                        | base64 encoded string |
| DATE, DATETIME, TIME, TIMESTAMP, YEAR    | string                |
| YEAR                                     | number                |
| BIT                                      | base64 encoded string |



## POST /{api-version}/{database}/{table}/pk-read

Is used to perform a primary key read operation. 

Assume we have the following table.

```
CREATE TABLE `my_table` (                                            
  `id0` int NOT NULL,                                                 
  `id1` int unsigned NOT NULL,                                        
  `col0` int DEFAULT NULL,                                            
  `col1` int unsigned DEFAULT NULL,                                   
  PRIMARY KEY (`id0`,`id1`)                                           
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

**Path Parameters:**

  - **api-version**: The current api version is 0.1.0
  - **database**: The database name to query from
  - **table**: The table name to query from

**Body:**

```json
{
  "filters": [
    {
      "column": "id0",
      "value": 0
    },
    {
      "column": "id1",
      "value": 0
    }
  ],
  "readColumns": [
    {
      "column": "col0",
      "dataReturnType": "default"
    },
    {
      "column": "col1",
      "dataReturnType": "default"
    }
  ],
  "operationId": "ABC123"
}

```

  - **filters**: (*required*) This is an array of objects one for each column that forms the primary key.
  - **readColumns**: (*optional*) This is used to perform projections. If it is omitted, all the columns of the table will be read
    - **dataReturnType**: (*optional*) This can be used to control in which format the data is returned, for example, hex, base64, etc. However, in this version (0.1.0) we only support the default return type.
  - **operationId**: (*optional*) It is a *string* parameter and it can be up to 64 characters long.

**Response**

```json
{
  "operationId": "ABC123",
  "data": {
    "col0": 123,
    "col1": 456
  }
}
```

## POST /{api-version}/batch

This is used to perform batched primary key read operations. 

**Path Parameters:**

  - **api-version**: The current api version is 0.1.0

**Body:**

The body here is a list of arbitrary pk-reads under the key *operations*:

```json
{
  "operations": [
    {
      "method": "POST",
      "relative-url": "my_database_1/my_table_1/pk-read",
      "body": {
        "filters": [
          {
            "column": "id0",
            "value": 0
          },
          {
            "column": "id1",
            "value": 0
          }
        ],
        "readColumns": [
          {
            "column": "col0",
            "dataReturnType": "default"
          },
          {
            "column": "col1",
            "dataReturnType": "default"
          }
        ],
        "operationId": "1"
      },
    },
    {
      "method": "POST",
      "relative-url": "my_database_2/my_table_2/pk-read",
      "body": {
        "filters": [
          {
            "column": "id0",
            "value": 1
          },
          {
            "column": "id1",
            "value": 1
          }
        ],
      },
    },
  ]
}
```

Additional parameters:
  - **relative-url**: (*required*) This represents the url the given pk-read would have in a single request (omitting the api-version).

**Response**

```json
[
  {
    "code": 200,
    "body": {
      "operationId": "1",
      "data": {
        "col0": 0,
        "col1": 0
      }
    }
  },
  {
    "code": 200,
    "body": {
      "data": {
        "col0": 1,
        "col1": 1
      }
    }
  }
]
```

## Security

Currently, the REST API server only supports [Hopsworks API Keys](https://docs.hopsworks.ai/feature-store-api/2.5.3/integrations/databricks/api_key/) for authentication and authorization. In the future, we plan to extend MySQL server users and privileges to the REST API.  Add the API key to the HTTP request using the **X-API-KEY** header. Ofcouse, you have to enable TLS when using API Keys. See, the configuration section for security related configuration parameters.  

## Configuration 
```json
{
        "Internal": {
                "APIVersion": "0.1.0",
                "BufferSize": 327680,
                "PreAllocatedBuffers": 1024,
                "GOMAXPROCS": -1
        },
        "REST": {
                "ServerIP": "localhost",
                "ServerPort": 4406
        },
        "GRPC": {
                "ServerIP": "localhost",
                "ServerPort": 5406
        },
        "RonDB": {
                "Mgmds": [
                        {
                                "IP": "localhost",
                                "Port": 1186
                        }
                ],
	            "ConnectionPoolSize": 1,
	            "NodeIDs": [],
	            "ConnectionRetries": 5,
	            "ConnectionRetryDelayInSec": 5,
	            "OpRetryOnTransientErrorsCount": 3,
	            "OpRetryInitialDelayInMS": 400
        },
        "Security": {
                "EnableTLS": true,
                "RequireAndVerifyClientCert": false,
                "CertificateFile": "",
                "PrivateKeyFile": "",
                "RootCACertFile": "",
                "UseHopsworksAPIKeys": true,
                "HopsworksAPIKeysCacheValiditySec": 3
        },
        "Log": {
                "Level": "info",
                "FilePath": "",
                "MaxSizeMB": 100,
                "MaxBackups": 10,
                "MaxAge": 30
        },
        "Testing": {
                "MySQL": {
                        "User": "rondb",
                        "Password": "rondb",
                        "Servers": [
                                {
                                        "IP": "localhost",
                                        "Port": 3306
                                }
                        ],
                }
        },
}
```

- **Internal**

  - **APIVersion:** Current version of the REST API. Current version is *0.1.0*

  - **BufferSize:** Size of the buffers that are used to pass requests/responses between the Go and C++ layers. The buffers should be large enough to accommodate any request/response. The default size is *327680* (32 KB). 

  - **PreAllocatedBuffers:** Numbers of buffers to preallocate. The default value is *1024*.

  - **GOMAXPROCS:** The GOMAXPROCS variable limits the number of operating system threads that can execute user-level Go code simultaneously.  The default value is -1, that is it does not change the current settings.

- **REST** 

  - **ServerIP:** Binds the REST server to this IP. The default value is *localhost*

  - **ServerPort:** REST server port. The default port is *4406*

- **GRPC** 

  - **ServerIP:** Binds the GRPC server to this IP. The default value is *localhost*

  - **ServerPort:** GRPC server port. The default port is *5406*

- **RonDB** 

  - **Mgmds:**

    - **IP:** RonDB management node IP. The default value is *localhost*.

    - **Port:** RonDB management node port. The default value is *1186*.

  - **ConnectionPoolSize:**  Connection pool size. Default 1. Note current implementation only supports 1 cluster connection

  - **NodeIDs:** This is an optional list of node ids to force the connections to be assigned to specific node ids. If this property is specified and connection pool size is not the default, the number of node ids must match the connection pool size

  - **ConnectionRetries:** Connection retries

  - **ConnectionRetryDelayInSec:** Connection retry delay in sec

  - **OpRetryOnTransientErrorsCount:** Number of times retry failed operations due to transient errors. 

  - **OpRetryInitialDelayInMS:** Initial delay used in expoential backoff for retrying failed operations.

- **Security:** REST server security settings 

  - **EnableTLS:** Enable/Disable TLS. The default value is *true*.
  
  - **RequireAndVerifyClientCert:**  Enable/Disable TLS client certificate requirement. The default value is *true*.

  - **RootCACertFile:**  Root CA file. Used in testing that use self-signed certificates. The default value is not set.
  
  - **CertificateFile:** Server certificate file. The default value is not set.
  
  - **PrivateKeyFile:** Server private key file. The default value is not set.

- **Log:** REST Server logging settings 

  - **Level:** log level, Supported levels are *panic, error, warn, info, debug,* and  *trace*. The default value is *info*.
  
  - **FilePath:** log file location. The default value is stdout.
  
  - **MaxSizeMB:** max log file size. The default value is *100*.
  
  - **MaxBackups:** max number of log files to store. The default value is *10*.
  
  - **MaxAge:** max-age of log files in days. The default value is *30*.

- **Testing:** MySQL server is only used for testing

  - **MySQL:** MySQL server is only used for testing
  
    - **User:** MySQL Server user. The default value is *rondb*.
    
    - **Password:** MySQL Server user password. The default value is *rondb*.
    
    - **Servers:**
  
      - **IP:** MySQL Server IP. The default value is *localhost*.
      
      - **Port:** MySQL Server port. The default value is *3306*.

