# Pluto Server

Pluto Server works as a plugin inside an [ArcadeDB](https://github.com/ArcadeData/arcadedb) server.

PlutoServer can be used via:

- HTTP/JSON.
- Postgres Driver. The [ArcadeDB](https://github.com/ArcadeData/arcadedb) server allows executing queries in SQL, Cypher, Gremlin
  and MongoDB language from any Postgres drivers

## HTTP/JSON API

**Authentication**

All the API must be authenticated with an ArcadeDB server user. By default, the user "root" with password "root:plutohash" is
created at startup.

By default, the JSON result is indented with 2 spaces. When the API user is a machine, you can specify the
header `pluto-compress: true` to compress the response and therefore increase performance.

### Bitcoin Database

#### Get a block

Returns a block by id (hash).

Syntax: `/block/{id}`

#### Get a transaction

Returns a transaction by id (hash).

Syntax: `/transaction/{id}`

#### Get an address and all its transactions

Returns an address by id (hash).

Syntax: `/address/{id}`

***Examples**

Returns the address in the database with hash "1ErrhTaqthRHtauyEMt2mGRszg2HFaJhND".

`curl "localhost:2480/pluto/address/1ErrhTaqthRHtauyEMt2mGRszg2HFaJhND" -H "Authorization: Basic cm9vdDpwbHV0b2hhc2g="`

```json
{"result": {
  "outputs": [],
  "inputs": [
    {
      "coinbase": false,
      "outputSum": 427494259,
      "purpose": "UNKNOWN",
      "lockTime": 0,
      "wtxid": "54c29f15e5fe81157074263d6f681b73f74aecbc75fb7a25149b97e8182126e3",
      "confidence": "UNKNOWN",
      "id": "54c29f15e5fe81157074263d6f681b73f74aecbc75fb7a25149b97e8182126e3",
      "value": 240515,
      "inputSum": 0 }, {
      "coinbase": false,
      "outputSum": 11886650117,
      "purpose": "UNKNOWN",
      "lockTime": 428567,
      "wtxid": "b0dfb8034b88f0cbe2b00e2d1ffc44bdec8cdca1c8ddda9de9155254e7e060e0",
      "confidence": "UNKNOWN",
      "id": "b0dfb8034b88f0cbe2b00e2d1ffc44bdec8cdca1c8ddda9de9155254e7e060e0",
      "value": 4600000,
      "inputSum": 0 
    }
  ],
  "label": "FaucetBOX",
  "hash": "1ErrhTaqthRHtauyEMt2mGRszg2HFaJhND"
  }
}
```

#### Get the addresses

Returns the addresses in the database. You can filter by label (optional). The default limit is 100. The timeout is 10 seconds.

Syntax: `/addresses[?label=<label>][&limit=10]`

**Examples**

Example: get the first address with label 'Binance'.

`curl "localhost:2480/pluto/addresses?label=Binance&limit=1" -H "Authorization: Basic cm9vdDpwbHV0b2hhc2g="`

```json
{
  "result": [
    {
      "outputs": [],
      "inputs": [],
      "label": "Binance",
      "hash": "bc1qwrne8nku0lk6c9dmtpd29ks6477u9262kcl29e"
    }
  ]
}  
```

## Start the server

```
> nohup bin/server.sh &
```

To stop the server simply kill the process (`kill <pid>`).

