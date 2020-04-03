<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 12:00
 */


namespace Oasis\Mlib\ODM\Spanner\Driver\Google;


use Google\Auth\Cache\SysVCacheItemPool;
use Google\Cloud\Core\Exception\GoogleException as GoogleExceptionAlias;
use Google\Cloud\Spanner\Database;
use Google\Cloud\Spanner\Session\CacheSessionPool;
use Google\Cloud\Spanner\SpannerClient;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\ODMException;
use Oasis\Mlib\ODM\Spanner\Schema\Structures\Column;
use Oasis\Mlib\ODM\Spanner\Schema\Structures\Table;

/**
 * Class SpannerDatabaseManager
 * @package Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers\Google
 */
class SpannerDatabaseManager
{
    /**
     * @var Database
     */
    private $database;


    /**
     * SpannerTable constructor.
     * @param  array  $dbConfig
     * @throws GoogleExceptionAlias
     */
    public function __construct(array $dbConfig)
    {
        $authCache    = new SysVCacheItemPool();
        $sessionCache = new SysVCacheItemPool(
            [
                // Use a different project identifier for ftok than the default
                'proj' => 'M',
            ]
        );

        $spanner = new SpannerClient(
            [
                'projectId' => $dbConfig['project_id'],
                'authCache' => $authCache,
            ]
        );

        // Get a Cloud Spanner instance by ID.
        $instance = $spanner->instance($dbConfig['instance_id']);

        // Get a Cloud Spanner database by ID.
        $sessionPool    = new CacheSessionPool(
            $sessionCache,
            [
                'minSessions' => 1,
                'maxSessions' => 1,
            ]
        );
        $this->database = $instance->database($dbConfig['database_id'], ['sessionPool' => $sessionPool]);

        // warm up will actually create the sessions for the first time.
        $sessionPool->warmup();
    }

    /**
     * @param $tableName
     * @return string|string[]
     */
    public static function convertTableName($tableName)
    {
        return str_replace('-', '_', $tableName);
    }

    public function listTables()
    {
        $ddlTexts = $this->database->ddl();

        foreach ($ddlTexts as $ddlText) {
            if (strpos($ddlText, 'CREATE TABLE') !== false) {
                $table = $this->getTableDefinitionFromSql($ddlText);
                print_r($table->__toArray());
            }
        }
    }

    public function testMatch()
    {
        $ddlTexts = "";//"CREATE TABLE odm_ut_users (\n  uid INT64 NOT NULL,\n  age INT64,\n  salary INT64,\n  ts INT64,\n  alias STRING(256) NOT NULL,\n  dummy STRING(256),\n  hometown STRING(256),\n  hometownPartition STRING(256),\n  name STRING(256),\n) PRIMARY KEY(uid)";
        $pattern1 = '#^CREATE\sTABLE\s(?P<table>\w+)\s\((?P<col>(\n\s+.*\,)+).*$#s';
        preg_match_all(
            $pattern1,
            $ddlTexts,
            $matches
        );

        if (empty($matches)) {
            echo "not matched".PHP_EOL;
        }
        else {
            // table name
            echo "table name: ".$matches['table'][0].PHP_EOL;

            // col
            $colStr     = $matches['col'][0];
            $patternCol = "/(?P<name>\w+)\s+(?P<type>\w+)\((?P<len>\d+)\)/";
            preg_match_all($patternCol, $colStr, $matchesCol);

            print_r($matchesCol['name']);
            print_r($matchesCol['type']);
            print_r($matchesCol['len']);


            $patternIntCol = "/(?P<name>\w+)\s+(INT64)/";
            preg_match_all($patternIntCol, $colStr, $matchesIntCol);
            print_r($matchesIntCol);

            echo PHP_EOL.$colStr.PHP_EOL;
        }
    }

    /**
     * @param $createTableSqlText
     * @return Table
     */
    public function getTableDefinitionFromSql($createTableSqlText)
    {
        $patternTable = '#^CREATE\sTABLE\s(?P<table>\w+)\s\((?P<col>(\n\s+.*\,)+).*$#s';
        preg_match_all(
            $patternTable,
            $createTableSqlText,
            $matches
        );

        if (empty($matches)) {
            throw new ODMException("No matches with sql: $createTableSqlText");
        }
        else {
            // extract table name
            $tableDDL = new Table();
            $tableDDL->setName($matches['table'][0]);

            // extract columns
            $colStr     = $matches['col'][0];
            $patternCol = "/(?P<name>\w+)\s+(?P<type>\w+)\((?P<len>\d+)\)/";
            preg_match_all($patternCol, $colStr, $matchesCol);

            $colCount = count($matchesCol['name']);

            if ($colCount > 0 && $colCount == count($matchesCol['type']) && $colCount == count($matchesCol['len'])) {
                for ($i = 0; $i < $colCount; $i++) {
                    $tableDDL->appendColumn(
                        (new Column())
                            ->setName($matchesCol['name'][$i])
                            ->setType($matchesCol['type'][$i])
                            ->setLength($matchesCol['len'][$i])
                    );
                }
            }
            else {
                throw new ODMException("Bad column define got: {$colStr}");
            }

            // extract INT64 type columns
            $patternIntCol = "/(?P<name>\w+)\s+(INT64)/";
            preg_match_all($patternIntCol, $colStr, $matchesIntCol);

            if (!empty($matchesIntCol) && !empty($matchesIntCol['name'])) {
                foreach ($matchesIntCol['name'] as $item) {
                    $tableDDL->appendColumn(
                        (new Column())
                            ->setName($item)
                            ->setType('INT64')
                            ->setLength(64)
                    );
                }
            }

            // extract BOOL type columns
            $patternBoolCol = "/(?P<name>\w+)\s+(BOOL)/";
            preg_match_all($patternBoolCol, $colStr, $matchesBoolCol);

            if (!empty($matchesBoolCol) && !empty($matchesBoolCol['name'])) {
                foreach ($matchesBoolCol['name'] as $item) {
                    $tableDDL->appendColumn(
                        (new Column())
                            ->setName($item)
                            ->setType('BOOL')
                            ->setLength(1)
                    );
                }
            }

            // extract DATE type columns
            $patternDateCol = "/(?P<name>\w+)\s+(DATE)/";
            preg_match_all($patternDateCol, $colStr, $matchesDateCol);

            if (!empty($matchesDateCol) && !empty($matchesDateCol['name'])) {
                foreach ($matchesDateCol['name'] as $item) {
                    $tableDDL->appendColumn(
                        (new Column())
                            ->setName($item)
                            ->setType('DATE')
                            ->setLength(1)
                    );
                }
            }

            // extract TIMESTAMP type columns
            $patternTimestampCol = "/(?P<name>\w+)\s+(TIMESTAMP)/";
            preg_match_all($patternTimestampCol, $colStr, $matchesTimestampCol);

            if (!empty($matchesTimestampCol) && !empty($matchesTimestampCol['name'])) {
                foreach ($matchesTimestampCol['name'] as $item) {
                    $tableDDL->appendColumn(
                        (new Column())
                            ->setName($item)
                            ->setType('TIMESTAMP')
                            ->setLength(1)
                    );
                }
            }

            return $tableDDL;
        }
    }


}
