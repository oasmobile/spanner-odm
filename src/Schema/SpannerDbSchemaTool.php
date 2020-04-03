<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 14:00
 */


namespace Oasis\Mlib\ODM\Spanner\Schema;


use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;
use Oasis\Mlib\ODM\Spanner\Driver\Google\SpannerDatabaseManager;

/**
 * Class SpannerDbSchemaTool
 * @package Oasis\Mlib\ODM\Dynamodb\DBAL\Schema
 */
class SpannerDbSchemaTool extends AbstractSchemaTool
{
    /**
     * @var array
     */
    protected $dbConfig;

    /**
     * @var SpannerDatabaseManager
     */
    private $spannerManager;

    /**
     * @param  array  $dbConfig
     * @return SpannerDbSchemaTool
     */
    public function setDbConfig($dbConfig)
    {
        $this->dbConfig = $dbConfig;

        return $this;
    }

    public function createSchema($skipExisting, $dryRun)
    {
        $this->getSpannerManager()->listTables();
//        $this->outputWrite("spanner createSchema");
    }

    public function updateSchema($isDryRun)
    {
        $this->getSpannerManager()->testMatch();
    }

    public function dropSchema()
    {
    }

    protected function getSpannerManager()
    {
        if ($this->spannerManager !== null) {
            return $this->spannerManager;
        }


        $this->spannerManager = new SpannerDatabaseManager(
            $this->dbConfig
        );

        return $this->spannerManager;
    }
}
