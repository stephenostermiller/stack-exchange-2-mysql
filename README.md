This imports a Stack Exchange data dump into a MySQL database.

### Requirements

- About 200 GB of free disk space, 100 for the compressed archive download and 100 for the database.
- Download a Stack Exchange data dump from https://archive.org/details/stackexchange
- Install dependencies: `sudo apt install mysql-server python3-mysql.connector p7zip-full`

### Usage

- Connect to MySQL and create a database and user for this data. For example:
   ```sql
   CREATE DATABASE IF NOT EXISTS stackexchange;
   CREATE USER IF NOT EXISTS 'stackexchange'@'%' IDENTIFIED WITH mysql_native_password  BY 'my-password';
   GRANT ALL ON stackexchange.* TO 'stackexchange'@'%';
   GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'stackexchange'@'%';
   FLUSH PRIVILEGES;
   ```
- Copy `default.ini` to `local.ini` and edit it with your database info.
- The list of sites must be loaded first because all other data depends on it: `./load.py /stackexchange/Sites.xml`
- Then load any or all of the other files. e.g. `./load.py /stackexchange/*.7z` or `./load.py /stackexchange/drones.stackexchange.com.7z`
- NOTE: MySQL can use a lot of disk space for query logs. By default, it usually has binary logging enabled and those logs are kept for 30 days. You might want to [disable binary logging](https://dba.stackexchange.com/questions/72770/disable-mysql-binary-logging-with-log-bin-variable) or periodically delete the logs with the SQL: `PURGE BINARY LOGS BEFORE NOW();`

### Features

- Automatically creates the database schema.
- All data loaded with `upsert`s. The same data can be loaded multiple times and will just overwrite itself. A newer version of the Stack Exchange dump can be imported on top of the old one and all the records will get updated.
- Data is committed to to the database in batches which makes it go much faster.
- All Stack Exchange sites are loaded into a **single** database. Each table uses a `SiteId` identifier as part of its primary key.
- `ENUM`s are used for all integer discriminator columns.
- `BIGINT`s are used for all keys
- All text columns are specified as `UTF-8`, specifically `CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`: 4 byte UTF-8 with case insensitive unicode sorting order.
- Uses compression within MySQL to minimize disk space usage.

