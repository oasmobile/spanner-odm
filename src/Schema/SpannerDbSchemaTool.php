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
use Oasis\Mlib\ODM\Spanner\Schema\Structures\ComparableItem;
use Oasis\Mlib\ODM\Spanner\Schema\Structures\Index;
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
        $tablesFromDB = $this->getSpannerManager()->listTables();

        if (empty($tablesFromDB)) {
            $this->outputWrite("No tables in database.");

            return;
        }

        $ddlResult = [];

        /**
         * @var string $name
         * @var Table $table
         */
        foreach ($tablesFromDB as $name => $table) {
            $table->setChangeType(ComparableItem::TO_DELETE);

            // drop table index before drop table itself
            foreach ($table->getIndexs() as $index) {
                $index->setChangeType(ComparableItem::TO_DELETE);
                $ddlResult[] = $index->toSql();
            }

            $ddlResult[] = $table->toSql();
        }

        if (!empty($ddlResult)) {
            foreach ($ddlResult as $sqlText) {
                $this->getSpannerManager()->runDDL(
                    $sqlText,
                    function ($text) {
                        $this->outputWrite($text);
                    }
                );
            }
        }
        $this->outputWrite('Done.');
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
        $tablesFromDB    = $this->getSpannerManager()->listTables();
        $tablesFromClass = $this->getTableInfoFromClasses();
        $compareResult   = $this->compareTableSet($tablesFromDB, $tablesFromClass);

        if ($dryRun) {
            foreach ($compareResult as $sqlText) {
                $this->outputWrite($sqlText);
            }
        }
        else {
            foreach ($compareResult as $sqlText) {
                $this->getSpannerManager()->runDDL(
                    $sqlText,
                    function ($text) {
                        $this->outputWrite($text);
                    }
                );
            }
            $this->outputWrite('Done.');
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
                    (new Column())->initWithObjectAttribute($name, $type)
                );
            }
            // set primary index
            $primaryKeyColumns = [
                $reflection->getFieldNameByPropertyName($reflection->getItemDefinition()->primaryIndex->hash),
            ];

            if (!empty($reflection->getItemDefinition()->primaryIndex->range)) {
                $primaryKeyColumns[] = $reflection->getFieldNameByPropertyName(
                    $reflection->getItemDefinition()->primaryIndex->hash
                );
            }

            $table->setPrimaryKeyColumns($primaryKeyColumns);

            // set other index
            foreach ($reflection->getItemDefinition()->globalSecondaryIndices as $globalSecondaryIndex) {
                $indexColumn   = [];
                $indexColumn[] = $reflection->getFieldNameByPropertyName($globalSecondaryIndex->hash);
                if (!empty($globalSecondaryIndex->range)) {
                    $indexColumn[] = $reflection->getFieldNameByPropertyName($globalSecondaryIndex->range);
                }
                $table->appendIndex(
                    (new Index())
                        ->setColumns($indexColumn)
                        ->setName($globalSecondaryIndex->name)
                );
            }

            // append to list
            $tableList[$table->getName()] = $table;
        }

        return $tableList;
    }

    protected function compareTableSet($tablesInDatabase, $tablesFromEntities)
    {
        $compareResult = [];

        /**
         * 1. find new tables and tables need to be changed
         *
         * @var string $name
         * @var Table $table
         */
        foreach ($tablesFromEntities as $name => $table) {
            if (!key_exists($name, $tablesInDatabase)) {
                $table->setChangeType(ComparableItem::IS_NEW);
                $compareResult[] = $table->toSql();
                // create table index
                foreach ($table->getIndexs() as $index) {
                    $index->setChangeType(ComparableItem::IS_NEW);
                    $compareResult[] = $index->toSql();
                }
            }
            else {
                $tableCompareRet = $this->compareTable($table, $tablesInDatabase[$name]);
                $compareResult   = array_merge($compareResult, $tableCompareRet);
            }
        }

        /**
         * 1. find tables to be removed
         *
         * @var string $name2
         * @var Table $table2
         */
        foreach ($tablesInDatabase as $name2 => $table2) {
            if (!key_exists($name2, $tablesFromEntities)) {
                $table2->setChangeType(ComparableItem::TO_DELETE);

                // drop table index before drop table itself
                foreach ($table2->getIndexs() as $index) {
                    $index->setChangeType(ComparableItem::TO_DELETE);
                    $compareResult[] = $index->toSql();
                }

                $compareResult[] = $table2->toSql();
            }
        }

        return $compareResult;
    }


    protected function compareTable(Table $tableFromEntity, Table $tableInDatabase)
    {
        $result = [];

        // find new columns or columns need to be changed
        foreach ($tableFromEntity->getColumns() as $column) {
            $changeType = $tableInDatabase->compareColumn($column);
            if ($changeType == ComparableItem::NO_CHANGE) {
                continue;
            }
            $column->setChangeType($changeType);
            $result[] = $column->toSql();
        }

        // find columns to be removed
        foreach ($tableInDatabase->getColumns() as $col) {
            if ($tableFromEntity->hasColumn($col) === false) {
                $col->setChangeType(ComparableItem::TO_DELETE);
                $result[] = $col->toSql();
            }
        }

        // find new index
        foreach ($tableFromEntity->getIndexs() as $index) {
            $changeType = $tableInDatabase->compareIndex($index);
            if ($changeType == ComparableItem::NO_CHANGE) {
                continue;
            }
            $index->setChangeType($changeType);
            $result[] = $index->toSql();
        }

        // find index to to delete
        foreach ($tableInDatabase->getIndexs() as $idx) {
            if ($tableFromEntity->hasIndex($idx) === false) {
                $idx->setChangeType(ComparableItem::TO_DELETE);
                $result[] = $idx->toSql();
            }
        }

        return $result;
    }
}
