<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 14:00
 */


namespace Oasis\Mlib\ODM\Spanner\Schema;


use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;
use Oasis\Mlib\ODM\Dynamodb\ItemReflection;
use Oasis\Mlib\ODM\Spanner\Driver\Google\SpannerDatabaseManager;
use Oasis\Mlib\ODM\Spanner\Driver\Google\SpannerTable;
use Oasis\Mlib\ODM\Spanner\Schema\Structures\Column;
use Oasis\Mlib\ODM\Spanner\Schema\Structures\Table;

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

    /**
     *
     * create or update tables
     *
     * @param $skipExisting
     * @param $dryRun
     */
    public function createSchema($skipExisting, $dryRun)
    {
        $this->updateTableSchemas($skipExisting, $dryRun);
    }

    /**
     * create or update tables
     *
     * @param $isDryRun
     */
    public function updateSchema($isDryRun)
    {
        $this->updateTableSchemas(true, $isDryRun);
    }

    /**
     * drop all tables
     */
    public function dropSchema()
    {
        $this->getSpannerManager()->testMatch();
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

    /**
     * @noinspection PhpUnusedParameterInspection
     */
    protected function updateTableSchemas($skipExisting, $dryRun)
    {
        $tablesFromDDL = $this->getSpannerManager()->listTables();

        /**
         * @var string $name
         * @var Table $table
         */
        foreach ($tablesFromDDL as $name => $table) {
            echo PHP_EOL.$name.PHP_EOL;
            print_r($table->__toArray());
        }
    }

    protected function getTableInfoFromClasses()
    {
        $classes   = $this->getManagedItemClasses();
        $tableList = [];

        /**
         * @var  $class
         * @var ItemReflection $reflection
         */
        foreach ($classes as $class => $reflection) {
            $itemDef = $reflection->getItemDefinition();
            if ($itemDef->projected) {
                $this->outputWrite(sprintf("Class %s is projected class, will not create table.", $class));
                continue;
            }

            $table = new Table();
            // set name
            $table->setName(
                SpannerTable::convertTableName(
                    $this->itemManager->getDefaultTablePrefix().$reflection->getTableName()
                )
            );

            // set columns
            foreach ($reflection->getAttributeTypes() as $name => $type) {
                $table->appendColumn(
                    (new Column())->initWithObjectAttribute($name,$type)
                );
            }
            // set index
            // todo

            // append to list
            $tableList[$table->getName()] = $table;
        }

        return $tableList;
    }
}
