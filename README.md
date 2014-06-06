# DBMS

This is where we will be storing per-device time-series data. It is currently only ideal for storing time series data for one building. More than one requires the DBMS *clients* to find a workaround (e.g. each device have a building's unique token appended/prepended to a device ID). It should be easy to implement a method so that devices from different homes can all share one DBMS endpoint. But that's for a later time.

## Running the DBMS

Be sure to have a MySQL server running.

Next, you also want to write a settings file in the `settings` folder. We'll call it `production.json`. It should look like:

```json
{
  "port": 4406,
  "mysql": {
    "user": "root",
    "password": "root",
    "host": "127.0.0.1",
    "database": "westhouse"
  }
}
```

Where, all of the properties of the `mysql` property are settings you would use to connect to *your* local MySQL server.

Then, to start the DBMS, you would run the following command:

```shell
node app
```

And the DBMS should start running.