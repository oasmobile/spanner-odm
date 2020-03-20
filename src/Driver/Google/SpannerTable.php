<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 12:00
 */


namespace Oasis\Mlib\ODM\Spanner\Driver\Google;


use Google\Auth\Cache\SysVCacheItemPool;
use Google\Cloud\Spanner\Database;
use Google\Cloud\Spanner\KeySet;
use Google\Cloud\Spanner\Session\CacheSessionPool;
use Google\Cloud\Spanner\SpannerClient;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\ODMException;

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

    public function __construct(array $dbConfig, $tableName, $attributeTypes = [])
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
        $this->attributeTypes = $attributeTypes;

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

    public static function convertTableName($tableName)
    {
        return str_replace('-', '_', $tableName);
    }


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

    /** @noinspection PhpUnusedParameterInspection */
    public function set(array $obj, $checkValues = [])
    {
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

    /** @noinspection PhpUnusedParameterInspection */
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

        return [];
    }

}
