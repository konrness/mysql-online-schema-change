<?php
date_default_timezone_set('UTC');
$packageAutoloader = __DIR__ . '/../../vendor/autoload.php';

require_once $packageAutoloader;

$logger = new \Osc\Logger(STDOUT, \Psr\Log\LogLevel::DEBUG);

$socket = "host=localhost";

$pdo = new \PDO("mysql:$socket;", 'root', 'password', array(
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
));

$database = 'osctest';
$table = 'osctest_table';

setUpTable($pdo, $database, $table);

populateTable($pdo, $database, $table);

$onlineSchemaChange = new \OnlineSchemaChangeRefactor(
    $pdo,
    $logger,
    $database,
    $table,
    "ADD COLUMN newcol int(8) DEFAULT 1 NOT NULL",
    null,
    OSC_FLAGS_ACCEPT_VERSION
);

$onlineSchemaChange->execute();


function setUpTable(\PDO $pdo, $database, $table)
{
    $pdo->query("CREATE DATABASE IF NOT EXISTS $database");
    $pdo->query("DROP TABLE IF EXISTS $database.$table");

    $create = "CREATE TABLE $database.$table (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `test` char(16) DEFAULT NULL,
  `date_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=106503 DEFAULT CHARSET=utf8;";

    $pdo->query($create);
}


function populateTable(\PDO $pdo, $database, $table)
{
    $query = "INSERT INTO $database.$table (test, date_time) VALUES";

    $insertQuery = array();
    $insertData = array();

    $n = 0;

    $insert = function() use(&$n, &$insertQuery, &$insertData, $pdo, $query){
        $sql = $query . implode(', ', $insertQuery);
        $stmt = $pdo->prepare($sql);
        $stmt->execute($insertData);
        $insertData = array();
        $insertQuery = array();
    };


    for($i = 0; $i <= 500000; $i++){

        $int = rand(1262055681,1262055681);

        $insertQuery[] = '(?,?)';
        $insertData[] = str_random(16);
        $insertData[] = date('Y-m-d H:i:s', $int);
        if(++$n >= 50000)
        {
            $insert();
            $n = 0;
        }
    }

    if($n > 0)
    {
        $insert();
    }
}

function str_random($length = 16)
{
    $pool = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

    return substr(str_shuffle(str_repeat($pool, 5)), 0, $length);
}