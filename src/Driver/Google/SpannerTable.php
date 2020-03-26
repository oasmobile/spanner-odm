<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 12:00
 */


namespace Oasis\Mlib\ODM\Spanner\Driver\Google;


use Exception;
use Google\Auth\Cache\SysVCacheItemPool;
use Google\Cloud\Core\Exception\AbortedException as AbortedExceptionAlias;
use Google\Cloud\Core\Exception\GoogleException as GoogleExceptionAlias;
use Google\Cloud\Spanner\Database;
use Google\Cloud\Spanner\KeySet;
use Google\Cloud\Spanner\Session\CacheSessionPool;
use Google\Cloud\Spanner\SpannerClient;
use Google\Cloud\Spanner\Transaction;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\DataConsistencyException;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\ODMException;
use Oasis\Mlib\ODM\Dynamodb\ItemReflection;

/**
 * Class SpannerTable
 * @package Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers\Google
 */
class SpannerTable
{
    /**
     * @var Database
     */
    private $database;

    /**
     * @var string
     */
    private $tableName;

    /**
     * @var array
     */
    private $attributeTypes;

    /**
     * @var ItemReflection
     */
    private $itemReflection;

    /**
     * SpannerTable constructor.
     * @param  array  $dbConfig
     * @param $tableName
     * @param  ItemReflection  $itemReflection
     * @throws GoogleExceptionAlias
     */
    public function __construct(array $dbConfig, $tableName, ItemReflection $itemReflection)
    {
        $authCache    = new SysVCacheItemPool();
        $sessionCache = new SysVCacheItemPool(
            [
                // Use a different project identifier for ftok than the default
                'proj' => 'B',
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
        $sessionConfig        = $this->getSessionPoolConfig($dbConfig);
        $sessionPool          = new CacheSessionPool(
            $sessionCache,
            [
                'minSessions' => $sessionConfig['minSessions'],
                'maxSessions' => $sessionConfig['maxSessions'],
            ]
        );
        $this->database       = $instance->database($dbConfig['database_id'], ['sessionPool' => $sessionPool]);
        $this->tableName      = self::convertTableName($tableName);
        $this->itemReflection = $itemReflection;
        $this->attributeTypes = $itemReflection->getAttributeTypes();

        // warm up will actually create the sessions for the first time.
        $sessionPool->warmup();
    }

    protected function getSessionPoolConfig($dbConfig)
    {
        // init default value
        $config = [
            'minSessions' => 10,
            'maxSessions' => 10,
        ];

        if (isset($dbConfig['cache_pool_sessions']) && ($num = intval($dbConfig['cache_pool_sessions'])) > 0) {
            $config['minSessions']  = $num;
            $config['max_sessions'] = $num;
        }

        return $config;
    }

    /**
     * @param $tableName
     * @return string|string[]
     */
    public static function convertTableName($tableName)
    {
        return str_replace('-', '_', $tableName);
    }

    /**
     * @param  array  $keys
     * @return array
     */
    public function batchGet(array $keys)
    {
        if (empty($keys)) {
            throw new ODMException("keys can not be empty for batchGet item action");
        }

        $returnSet = [];
        $keyValues = [];
        foreach ($keys as $key) {
            if (!empty($key)) {
                $keyValues[] = array_values($key);
            }
        }

        if (empty($keyValues)) {
            return $returnSet;
        }

        $keySet  = new KeySet(
            [
                'keys' => $keyValues,
            ]
        );
        $results = $this->database->read(
            $this->tableName,
            $keySet,
            array_keys($this->attributeTypes)
        );

        foreach ($results->rows() as $row) {
            $returnSet[] = $row;
        }

        return $returnSet;
    }

    /**
     * @param  array  $obj
     * @param  array  $checkValues
     * @return bool
     * @throws AbortedExceptionAlias
     */
    public function set(array $obj, $checkValues = [])
    {
        if (empty($checkValues)) {
            $this->database->transaction(['singleUse' => true])
                ->insertOrUpdateBatch(
                    $this->tableName,
                    [
                        $obj,
                    ]
                )
                ->commit();

            return true;
        }

        // Do check and set in transaction
        $tableName = $this->tableName;
        $columns   = array_keys($this->attributeTypes);
        $className = $this->itemReflection->getItemClass();
        $keySet    = new KeySet(
            [
                'keys' => array_values($this->itemReflection->getPrimaryKeys($obj)),
            ]
        );

        $this->database->runTransaction(
            function (Transaction $t) use ($obj, $tableName, $columns, $keySet, $checkValues, $className) {
                $rawData = [];

                try {
                    $readRet = $t->read(
                        $tableName,
                        $keySet,
                        $columns
                    );

                    $rawData = $readRet->rows()->current();
                } catch (Exception $exception) {
                    // do nothing
                }

                // compare values
                if (!empty($rawData)) {
                    foreach ($checkValues as $k => $val) {
                        if ($rawData[$k] !== $val) {
                            throw new DataConsistencyException(
                                "Item updated elsewhere! type = {$className}"
                            );
                        }
                    }
                }

                // do commit
                $t->insertOrUpdateBatch(
                    $tableName,
                    [
                        $obj,
                    ]
                );
                $t->commit();
            }
        );

        return true;
    }

    /**
     * @param  array  $keys
     * @return mixed|null
     */
    public function get(array $keys)
    {
        if (empty($keys)) {
            throw new ODMException("keys can not be empty for get item action");
        }

        $keySet  = new KeySet(
            [
                'keys' => array_values($keys),
            ]
        );
        $results = $this->database->read(
            $this->tableName,
            $keySet,
            array_keys($this->attributeTypes)
        );

        if (!empty($row = $results->rows()->current())) {
            return $row;
        }

        return null;
    }

    /**
     * @param  array  $objs
     * @return bool
     * @throws AbortedExceptionAlias
     */
    public function batchPut(array $objs)
    {
        $this->database->transaction(['singleUse' => true])
            ->insertOrUpdateBatch(
                $this->tableName,
                $objs
            )
            ->commit();

        return true;
    }

    /**
     * @param  array  $objs
     * @return bool
     * @throws AbortedExceptionAlias
     */
    public function batchDelete(array $objs)
    {
        $t = $this->database->transaction(['singleUse' => true]);

        foreach ($objs as $obj) {
            $t->delete(
                $this->tableName,
                new KeySet(
                    [
                        'keys' => array_values($this->itemReflection->getPrimaryKeys($obj)),
                    ]
                )
            );
        }
        $t->commit();

        return true;
    }

    /**
     * @param $keyConditions
     * @param  array  $fieldsMapping
     * @param  array  $paramsMapping
     * @param  bool  $fetchCountValue
     * @param  int  $evaluationLimit
     * @return array|int
     */
    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $fetchCountValue = false,
        $evaluationLimit = 0
    ) {
        // Get query condition expression for sql
        $getConditionExpression = function ($keyConditions, $fieldsMapping) {
            if (empty($keyConditions)) {
                return '1=1';
            }

            $replaceSearch    = array_keys($fieldsMapping);
            $replaceReplace   = array_values($fieldsMapping);
            $replaceSearch[]  = ':';
            $replaceReplace[] = '@';

            return str_replace($replaceSearch, $replaceReplace, $keyConditions);
        };

        // Get query condition value
        $getParameterValues = function ($paramsMapping) {
            $paraVal = [];
            foreach ($paramsMapping as $k => $v) {
                $paraVal[ltrim($k, ':')] = $v;
            }

            return $paraVal;
        };

        if ($fetchCountValue === true) {
            $queryCol = "count(*)";
        }
        else {
            $queryCol = "*";
        }

        if ($fetchCountValue === false && $evaluationLimit > 0) {
            $queryResultLimit = "limit {$evaluationLimit}";
        }
        else {
            $queryResultLimit = '';
        }

        $querySql = sprintf(
            "SELECT %s FROM %s WHERE %s %s",
            $queryCol,
            $this->tableName,
            $getConditionExpression($keyConditions, $fieldsMapping),
            $queryResultLimit
        );

        $results = $this->database->execute(
            $querySql,
            [
                'parameters' => $getParameterValues($paramsMapping),
            ]
        );

        if ($fetchCountValue) {
            if (!empty($row = $results->rows()->current())) {
                return $row[0];
            }
            else {
                return 0;
            }
        }
        else {
            $returnSet = [];
            foreach ($results->rows() as $row) {
                $returnSet[] = $row;
            }

            return $returnSet;
        }
    }

    /**
     * @param  string  $hashKeyName
     * @param  array  $hashKeyValues
     * @param  string  $rangeKeyConditions
     * @param  array  $fieldsMapping
     * @param  array  $paramsMapping
     * @param  int  $evaluationLimit
     * @return array
     */
    public function multiQuery(
        $hashKeyName,
        array $hashKeyValues,
        $rangeKeyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $evaluationLimit = 0
    ) {
        // Get query condition expression for sql
        $getConditionExpression = function ($keyConditions, $fieldsMapping) {
            $replaceSearch    = array_keys($fieldsMapping);
            $replaceReplace   = array_values($fieldsMapping);
            $replaceSearch[]  = ':';
            $replaceReplace[] = '@';

            return str_replace($replaceSearch, $replaceReplace, $keyConditions);
        };

        // Get hash key condition
        $getHashKeyConditionExpression = function ($hashKeyName, array $hashKeyValues) {
            $ceStr = '';
            if (empty($hashKeyValues)) {
                return $ceStr;
            }

            foreach ($hashKeyValues as $value) {
                $ceStr .= "'{$value}',";
            }

            return sprintf(
                "%s in (%s)",
                $hashKeyName,
                rtrim($ceStr, ',')
            );
        };

        // Get query condition value
        $getParameterValues = function ($paramsMapping) {
            $paraVal = [];
            foreach ($paramsMapping as $k => $v) {
                $paraVal[ltrim($k, ':')] = $v;
            }

            return $paraVal;
        };


        // get query condition expression
        $queryExpression         = '1=1';
        $hasKeyQueryExpression   = $getHashKeyConditionExpression($hashKeyName, $hashKeyValues);
        $rangeKeyQueryExpression = $getConditionExpression($rangeKeyConditions, $fieldsMapping);

        if (!empty($hasKeyQueryExpression)) {
            $queryExpression .= " AND {$hasKeyQueryExpression} ";
        }

        if (!empty($rangeKeyQueryExpression)) {
            $queryExpression .= " AND {$rangeKeyQueryExpression} ";
        }

        // query limit
        if ($evaluationLimit > 0) {
            $queryResultLimit = "limit {$evaluationLimit}";
        }
        else {
            $queryResultLimit = '';
        }

        $querySql = sprintf(
            "SELECT * FROM %s WHERE %s %s",
            $this->tableName,
            $queryExpression,
            $queryResultLimit
        );

        $results = $this->database->execute(
            $querySql,
            [
                'parameters' => $getParameterValues($paramsMapping),
            ]
        );

        $returnSet = [];
        foreach ($results->rows() as $row) {
            $returnSet[] = $row;
        }

        return $returnSet;
    }

}
