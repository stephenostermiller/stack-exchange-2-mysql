This imports a Stack Exchange data dump into a MySQL database.

### Requirements

- About 300 GB of free disk space, 100 for the archive download and 200 for the database.
- Download a Stack Exchange data dump from https://archive.org/details/stackexchange
- Install dependencies: `sudo apt install mysql-server python3-mysql.connector p7zip-full`

### Usage

1. Connect to MySQL and create a database and user. For example:
   ```sql
   CREATE DATABASE IF NOT EXISTS stackexchange;
   CREATE USER IF NOT EXISTS 'stackexchange'@'%' IDENTIFIED WITH mysql_native_password  BY 'my-password';
   GRANT ALL ON stackexchange.* TO 'stackexchange'@'%';
   ```
1. Configure the new database not to keep binary logs. These logs required several times the amount of disk space as the actual data:
   ```sh
   echo -e '[mysqld]\nbinlog-ignore-db=stackexchange' | sudo tee /etc/mysql/conf.d/stackexchange.cnf
   sudo service mysql restart
   ```
1. Copy `default.ini` to `local.ini` and edit it with your database info.
1. The list of sites must be loaded first because all other data depends on it: `./load.py /stackexchange/Sites.xml`
1. Then load any or all of the other files. e.g. `./load.py /stackexchange/*.7z` or `./load.py /stackexchange/drones.stackexchange.com.7z`

### Features

- Automatically creates the database schema.
- All data loaded with `upsert`s. The same data can be loaded multiple times and will just overwrite itself. A newer version of the Stack Exchange dump can be imported on top of the old one and all the records will get updated.
- Data is committed to to the database in batches which makes it go much faster.
- All Stack Exchange sites are loaded into a **single** database. Each table uses a `SiteId` identifier as part of its primary key.
- `ENUM`s are used for all integer discriminator columns.
- `BIGINT`s are used for all numeric values
- All text columns are specified as `UTF-8`, specifically `CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`: 4 byte UTF-8 with case insensitive unicode sorting order.
- Uses compression within MySQL to minimize disk space usage.

### Advanced Usage

- You can specify specific tables to load instead of loading all the tables: `./load.py /stackexchange/drones.stackexchange.com.7z Posts Users`. Data from other tables (`Badges`, `Comments`, `PostHistory`, `PostLinks`, `Tags`, and `Votes`) will be skipped.
- If you need to interrupt the load and want to resume from where it left off, you can specify the resume point: `./load.py /stackexchange/drones.stackexchange.com.7z "drones PostHistory 4129"`

### FAQ

#### How long does it take?
Loading the full dump can take several days. Loading just a small site (such as "drones") can be done in seconds.

#### When was this last tested?
This was tested with the June 2022 data dump on Ubuntu 20.04.4 LTS with MySQL 8.0.29.
