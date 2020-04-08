<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-09-19
 * Time: 11:08
 */

use Oasis\Mlib\ODM\Dynamodb\Console\ConsoleHelper;
use Oasis\Mlib\ODM\Dynamodb\ItemManager;
use Oasis\Mlib\ODM\Spanner\Driver\SpannerDbConnection;
use Oasis\Mlib\ODM\Spanner\Ut\UTConfig;

// replace with file to your own project bootstrap
require_once __DIR__ . '/ut/bootstrap.php';

$im = new ItemManager(
    new SpannerDbConnection(UTConfig::$dbConfig), UTConfig::$tablePrefix, __DIR__."/ut/cache", true
);
$im->addNamespace('Oasis\Mlib\ODM\Spanner\Ut', __DIR__ . "/ut");

return new ConsoleHelper($im);
