<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 12:00
 */

namespace Oasis\Mlib\ODM\Spanner\Driver;


use Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers\AbstractDbConnection;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\ODMException;
use Oasis\Mlib\ODM\Dynamodb\ItemManager;
use Oasis\Mlib\ODM\Spanner\Driver\Google\SpannerTable;
use Oasis\Mlib\ODM\Spanner\Schema\SpannerDbSchemaTool;

/**
 * Class SpannerDbConnection
 * @package Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers
 */
class SpannerDbConnection extends AbstractDbConnection
{
    /**
     * @var SpannerTable
     */
    protected $spannerTable = null;

    public function batchGet(
        array $keys,
        $isConsistentRead = false,
        $concurrency = 10,
        $projectedFields = [],
        $keyIsTyped = false,
        $retryDelay = 0,
        $maxDelay = 15000
    ) {
        return $this->getSpannerTable()->batchGet($keys);
    }

    protected function getSpannerTable()
    {
        if ($this->spannerTable !== null) {
            return $this->spannerTable;
        }
        if (empty($this->tableName)) {
            throw new ODMException("Unknown table name to initialize spanner client");
        }

        if ($this->itemReflection === null) {
            throw new ODMException("Unknown item reflection to initialize spanner client");
        }

        $this->spannerTable = new SpannerTable(
            $this->dbConfig,
            $this->tableName,
            $this->itemReflection
        );

        return $this->spannerTable;
    }

    public function batchDelete(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        return $this->spannerTable->batchDelete($objs);
    }

    public function batchPut(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        $this->spannerTable->batchPut($objs);
    }

    public function set(array $obj, $checkValues = [])
    {
        return $this->getSpannerTable()->set($obj, $checkValues);
    }

    public function get(array $keys, $is_consistent_read = false, $projectedFields = [])
    {
        return $this->getSpannerTable()->get($keys);
    }

    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        &$lastKey = null,
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        return $this->getSpannerTable()->query(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping,
            false,
            $evaluationLimit
        );
    }

    public function queryAndRun(
        callable $callback,
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $resultSet = $this->getSpannerTable()->query($keyConditions, $fieldsMapping, $paramsMapping);
        //
        if (!empty($resultSet)) {
            $stoppedByCallback = false;
            foreach ($resultSet as $item) {
                if ($stoppedByCallback === true) {
                    return;
                }

                $ret = call_user_func($callback, $item);
                if ($ret === false) {
                    $stoppedByCallback = true;
                }
            }
        }
    }

    public function queryCount(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $isConsistentRead = false,
        $isAscendingOrder = true
    ) {
        return $this->getSpannerTable()->query($keyConditions, $fieldsMapping, $paramsMapping, true);
    }

    public function multiQueryAndRun(
        callable $callback,
        $hashKeyName,
        $hashKeyValues,
        $rangeKeyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $concurrency = 10,
        $projectedFields = []
    ) {
        $resultSet = $this->getSpannerTable()->multiQuery(
            $hashKeyName,
            $hashKeyValues,
            $rangeKeyConditions,
            $fieldsMapping,
            $paramsMapping,
            $evaluationLimit
        );
        //
        if (!empty($resultSet)) {
            $stoppedByCallback = false;
            foreach ($resultSet as $item) {
                if ($stoppedByCallback === true) {
                    return;
                }

                $ret = call_user_func($callback, $item);
                if ($ret === false) {
                    $stoppedByCallback = true;
                }
            }
        }
    }

    public function scan(
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        &$lastKey = null,
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        return $this->getSpannerTable()->query(
            $filterExpression,
            $fieldsMapping,
            $paramsMapping,
            false,
            $evaluationLimit
        );
    }

    public function scanAndRun(
        callable $callback,
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $resultSet = $this->getSpannerTable()->query($filterExpression, $fieldsMapping, $paramsMapping);
        //
        if (!empty($resultSet)) {
            $stoppedByCallback = false;
            foreach ($resultSet as $item) {
                if ($stoppedByCallback === true) {
                    return;
                }

                $ret = call_user_func($callback, $item);
                if ($ret === false) {
                    $stoppedByCallback = true;
                }
            }
        }
    }

    public function parallelScanAndRun(
        $parallel,
        callable $callback,
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $resultSet = $this->getSpannerTable()->query($filterExpression, $fieldsMapping, $paramsMapping);
        //
        if (!empty($resultSet)) {
            $stoppedByCallback = false;
            foreach ($resultSet as $item) {
                if ($stoppedByCallback === true) {
                    return;
                }

                $ret = call_user_func($callback, $item);
                if ($ret === false) {
                    $stoppedByCallback = true;
                }
            }
        }
    }

    public function scanCount(
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $parallel = 10
    ) {
        return $this->getSpannerTable()->query($filterExpression, $fieldsMapping, $paramsMapping, true);
    }

    /**
     * @inheritDoc
     */
    public function getSchemaTool(ItemManager $im, $classReflections, callable $outputFunction = null)
    {
        return (new SpannerDbSchemaTool($im, $classReflections, $outputFunction))->setDbConfig($this->dbConfig);
    }
}
