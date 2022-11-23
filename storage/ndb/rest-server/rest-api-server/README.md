# RonDB REST API Server 

Currently, the REST API server only supports batched and non-batched  primary key operations. Default mappings of MySQL data types to JSON data types are as follows


| MySQL Data Type                          | JSON Data Type        |
| ---------------------------------------- | --------------------- |
| TINYINT, SMALLINT MEDIUMINT, INT, BIGINT | number                |
| FLOAT, DOUBLE, DECIMAL                   | number                |
| CHAR, VARCHAR                            | escaped string        |
| BINARY, VARBINARY                        | base64 encoded string |
| DATE, DATETIME, TIME, TIMESTAMP, YEAR    | string                |
| YEAR                                     | number                |
| BIT                                      | base64 encoded string |



## POST /0.1.0/{database}/{table}/pk-read

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

  - *api-version* : current api version is 0.1.0
  - *database* : database name
  - *table* : table name

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

  - **filters** : This is mandatory parameter. It is an array of objects one for each column that forms the primary key. 
  - **readColumns** : It is an optional parameter that is used to perform projections. If it is omitted then all the columns of the table will be read
    - **dataReturnType** : It is an optional parameter. It can be used to control in which format the data is returned, for example, hex, base64, etc. However, in this version (0.1.0) we only support the default return type.  
  - **operationId** : It is an optional parameter. It is a *string* parameter and it can be up to 64 characters long. 

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

## POST /0.1.0/batch

Is used to perform batched primary key read operations. 

**Path Parameters:**

  - *api-version* : current api version is 0.1.0

**Body:**

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
    "RestServer": {
        "IP": "localhost",
        "Port": 4406,
        "APIVersion": "0.1.0",
        "BufferSize": 327680,
        "PreAllocatedBuffers": 1024,
        "GOMAXPROCS": -1
    },
    "RonDBConfig": {
        "IP": "localhost",
        "Port": 1186
    },
    "MySQLServer": {
        "IP": "localhost",
        "Port": 3306,
        "User": "rondb",
        "Password": "rondb"
    },
    "Security": {
        "EnableTLS": true,
        "RequireAndVerifyClientCert": true,
        "CertificateFile": "",
        "PrivateKeyFile": ""
    },
    "Log": {
        "Level": "info",
        "FilePath": "",
        "MaxSizeMB": 100,
        "MaxBackups": 10,
        "MaxAge": 30
    }
}
```

 - **RestServer** 

   - **IP:** Binds the REST server to this IP. The default value is *localhost*
  
   - **Port:** REST server port. The default port is *4406*
   
   - **APIVersion:** Current version of the REST API. Current version is *0.1.0*
   
   - **BufferSize:** Size of the buffers that are used to pass requests/responses between the Go and C++ layers. The buffers should be large enough to accommodate any request/response. The default size is *327680* (32 KB). 

   - **PreAllocatedBuffers:** Numbers of buffers to preallocate. The default value is *1024*.
   
   - **GOMAXPROCS:** The GOMAXPROCS variable limits the number of operating system threads that can execute user-level Go code simultaneously.  The default value is -1, that is it does not change the current settings.

- **RonDBConfig**

   - **IP:** RonDB management node IP. The default value is *localhost*.
   
   - **Port:** RonDB management node port. The default value is *1186*.
  
 - **MySQLServer:** configuration. MySQL server is only used for testing
  
   - **IP:** MySQL Server IP. The default value is *localhost*.
   
   - **Port:** MySQL Server port. The default value is *3306*.
   
   - **User:** MySQL Server user. The default value is *rondb*.
   
   - **Password:** MySQL Server user password. The default value is *rondb*.

 - **Security:** REST server security settings 
  
   - **EnableTLS:** Enable/Disable TLS. The default value is *true*.
   
   - **RequireAndVerifyClientCert:**  Enable/Disable TLS client certificate requirement. The default value is *true*.

   - **RootCACertFile:**  Root CA file. Used in testing that use self-signed certificates. The default value is not set.
   
   - **CertificateFile:** Server certificate file. The default value is not set.
   
   - **PrivateKeyFile:** Server private key file. The default value is not set.

 - **Log:** REST Server logging settings 
  
   - **Level:** log level, Supported levels are *panic, error, warn, info, debug,* and  *trace*. The default value is *info*.
   
   - **FilePath:** log file location. The default value is not set.
   
   - **MaxSizeMB:** max log file size. The default value is *100*.
   
   - **MaxBackups:** max number of log files to store. The default value is *10*.
   
   - **MaxAge:** max-age of log files in days. The default value is *30*.
