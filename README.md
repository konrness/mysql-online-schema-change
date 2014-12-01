MySQL Online Schema Change
=======

### WARNING - This package affects data in database tables. Use with extreme caution and back up databases before running the command against them.

### What is it?
MySQL OSC is a tool for making changes to large databases without taking the database offline.

Inserts, updates and deletes to the table can still take place while the change is happening. This tool works against both InnoDB and MyISAM tables.


### How does it work
The package makes an empty copy of your original table, and runs the give alter statement against that table.

It then uses MySQLs `SELECT INTO OUTFILE` to dump data from the original table into files which are loaded into the new table

Triggers are set up to catch changes made to the old table while this is happening, and the changes are replayed against the new table
once all files have been loaded.

The package then locks the tables, replays any new changes one last time before renaming the tables using `ALTER TABLE` syntax which works on locked tables (unlike `RENAME TABLE`)

### How do I use it?

Download the phar archive directly

    curl -fsSLo online-schema-change https://github.com/mrjgreen/mysql-online-schema-change/raw/master/build/online-schema-change.phar
    chmod a+x online-schema-change


Call directly, eg.

    // add -v or -vv for additional output
    ./online-schema-change database table "alter table statement" --user jsmith --password pw0rd


This tool uses a modified version of the facebook OSC script http://bazaar.launchpad.net/~mysqlatfacebook/mysqlatfacebook/tools/annotate/head:/osc/OnlineSchemaChange.php

It has been updated to use PDO and a PSR logger, and wrapped in a console command tool.
