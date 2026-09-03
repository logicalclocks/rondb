{
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {
        "component": {
            "type": "container",
            "name": "hopsworks/rondb",
            "version": "__RONDB_VERSION__"
        },
        "properties": [
            {
                "name": "works.hops.rondb:source-of-truth",
                "value": "build_scripts/release_scripts/build-docker-image/resources/rondb.cdx.json.tpl - the mysql_server version below is the upstream MySQL base of this release branch and must be updated as part of every upstream MySQL merge"
            }
        ]
    },
    "components": [
        {
            "type": "application",
            "name": "rondb",
            "version": "__RONDB_VERSION__",
            "supplier": {
                "name": "Hopsworks"
            },
            "purl": "pkg:generic/hopsworks/rondb@__RONDB_VERSION__"
        },
        {
            "type": "application",
            "name": "mysql_server",
            "version": "8.4.11",
            "supplier": {
                "name": "Oracle"
            },
            "cpe": "cpe:2.3:a:oracle:mysql_server:8.4.11:*:*:*:*:*:*:*"
        }
    ]
}
