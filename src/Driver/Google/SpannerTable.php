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
     * @param  bool  $isConsistentRead
     * @param  int  $concurrency
     * @param  array  $projectedFields
     * @param  bool  $keyIsTyped
     * @param  int  $retryDelay
     * @param  int  $maxDelay
     * @return array
     */
    public function batchGet(
        array $keys,
        $isConsistentRead = false,
        $concurrency = 10,
        $projectedFields = [],
        $keyIsTyped = false,
        $retryDelay = 0,
        $maxDelay = 15000
    ) {
        return [$keys, $isConsistentRead, $concurrency, $projectedFields, $keyIsTyped, $retryDelay, $maxDelay];
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
     * @param  bool  $is_consistent_read
     * @param  array  $projectedFields
     * @return mixed|null
     * @noinspection PhpUnusedParameterInspection
     */
    public function get(array $keys, $is_consistent_read = false, $projectedFields = [])
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

        foreach ($results->rows() as $row) {
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

}
