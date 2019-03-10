# osm-apidb-writer
Module for writing OSC and OSM files (OpenStreetMap file format) into an OSM API postgresql database

## Vulnerability Warning
Currently, the module does not make any validation on the input, and uses string concatinations to build SQL queries, therefore, it is vulnerable to an SQL Injection attack.  
**DO NOT USE THIS MODULE ON AN UNTRUSTED INPUT!**  
This module should be used only in an internal system, with known inputs generated by a known tool such as Osmosis  

## Usage Example
```js
const OsmApidbWriter = require('osm-apidb-writer');
const fs = require('fs');

// this module works on objects generated using the osm-object-stream module
const OsmObjectStream = require('osm-object-stream');

const osmFileStream = fs.createReadStream('/path/to/file.osc');
const osmObjectStream = new OsmObjectStream({});
const osmApidbWriter = new OsmApidbWriter({
    pgConnectionProperties: {
        host: 'pg_host',
        user: 'pg_user',
        password: 'pg_password',
        database: 'pg_db'
    }
});

osmFileStream.pipe(osmObjectStream);
osmObjectStream.pipe(osmApidbWriter);
```