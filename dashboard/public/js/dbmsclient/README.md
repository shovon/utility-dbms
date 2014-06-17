# dbmsclient

This is a helper library for the DBMS client. It's intended to log a DBMS user in, and maintain session, without having to log back in again.

## Example Usage

```javascript
// Creat a new DBMS client.
var client = new DBMSClient('username', 'password', 'http://host');

// Get all data.
client.getData('energy_consumption', {}, function (err, data) {
  if (err) {
    // Handle error however you like, here.
  }

  // Do whatever you like with the data here.
});
```

## Class: DBMSClient

### DBMSClient(username, password, host)

- `username` String
- `password` String
- `host` String - The hostname, which includes the protocol (e.g. http or https), remote host name (e.g. IP address, or a fully-qualified domain name (FQDN)), and an optional port number. E.g. `'http://dbms:4406'.

Creates a new instances of the DBMSClient class.

### DBMSClient#getData(timeSeries, options, callback)

- `timeSeries` String
- `options` Object
- `callback` Function - hash the following parameters: `err` and `data`

Gets data stored in the time series, specified by `timeSeries`. Additionally, you can specify filters, by defining specific properties in the `options` object.

A lot of the `options` object's properties are an attempt of being a mapping of the original HTTP API calls. There are some discrepancies for the sake of simplifying the task of querying.

| Property | Description |
| --- | --- |
| `func` | The aggregate function to apply to the time series data. Accepted aggregate functions are `mean`, `min`, `max`, and `sum`. When omitted, `func` defaults to mean. |
| `interval` | The time interval to which the aggregate data into. Can be either per row (technically per second), per minute (m), hourly (h), daily (d), weekly (w), monthly (mo), and yearly (y). The interval typically takes the form of `<amount><interval token>`, e.g. `12h` for "twelve hours", or `60d` for "sixty days", or `10` for "10 seconds" (N.B. the omitted token). When omitted, `interval` defaults to `1` |
| `devices` | The list of devices to aggregate, or to filter out from the aggregate among all the devices in the series. This is a key-value pair, which have the property `ids` and exclude. `ids` is a list of all physical device IDs, where as exclude is a boolean; when set to true, the devices list in `ids` will serve to filter out the specified devices from the aggregate. When omitted, `devices` defaults to the IDs of all the devices in the specified time series |
| `from` | Get all data since the specified time in `from`. The value in `from` can either be an ISO 8601 string, or a shortcode (which will be explained, further, below) |
| `to` | Get all data up to a the specified time in `to`. The value in `to` can either be an ISO 8601 time string, or a shortcode |
| `groupbyhour` | Get all data grouped together, on an hourly basis. And hence, the cardinality of the returned value is going to be no larger than twenty four |

With the shortcode, you should be able to query *back* a certain time. So, for instance, if you want to query data as far back as a month ago, you would have a short code that looks like:

```
1m
```

A month and twelve days back?

```
1m 12d
```

A one and a half year back?

```
1y 6mo
```

So, for example, your `getData` call would look like the following, if you wanted to get `energy_consumption` data as far back as two years, four months, ten days, six hours, and fifteen minutes ago.

```javascript
// Where client is an instance of `DBMSClient`
client.getData('energy_consumption', {
  from: '1y 4mo 10d 6h 15m'
}, function (err, data) {
  if (err) { /* Do whatever, when an error occurs. */ }
  // Do whatever with the data here.
});
```

This is an asynchronous function, and `callback` will be called once the function has finished running its query. The data will be available in the `data` property. If an error occured, no error will be thrown, however, when `callback` is called, `data` will not be defined, and `err` will be an error object.