<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Table
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Table extends ComparableItem
{
    /**
     * @var string
     */
    private $name = '';

    /**
     * @var Column[]
     */
    private $columns = [];

    /**
     * @var Index[]
     */
    private $indexs = [];

    /**
     * @var array
     */
    private $primaryKeyColumns = [];


    /**
     * @var array
     */
    private $columnMap = [];

    /**
     * @var array
     */
    private $indexMap = [];

    /**
     * @return array
     */
    public function getPrimaryKeyColumns()
    {
        return $this->primaryKeyColumns;
    }

    /**
     * @param  array  $primaryKeyColumns
     */
    public function setPrimaryKeyColumns($primaryKeyColumns)
    {
        $this->primaryKeyColumns = $primaryKeyColumns;
    }

    /**
     * @return Index[]
     */
    public function getIndexs()
    {
        return $this->indexs;
    }

    /**
     * @param  Index[]  $indexs
     * @return Table
     */
    public function setIndexs($indexs)
    {
        $this->indexs = $indexs;

        return $this;
    }

    /**
     * @param  Index  $index
     * @return Table
     */
    public function appendIndex(Index $index)
    {
        $index->setTableName($this->name);
        $this->indexs[] = $index;

        return $this;
    }


    /**
     * @return Column[]
     */
    public function getColumns()
    {
        return $this->columns;
    }


    /**
     * @param  Column[]  $columns
     * @return Table
     */
    public function setColumns($columns)
    {
        $this->columns = $columns;

        return $this;
    }

    /**
     * @param  Column  $column
     * @return Table
     */
    public function appendColumn(Column $column)
    {
        $column->setTableName($this->name);
        $this->columns[] = $column;

        return $this;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param  string  $name
     * @return Table
     */
    public function setName($name)
    {
        $this->name = $name;

        return $this;
    }

    /**
     * @return array
     */
    public function __toArray()
    {
        $table = [
            'name'        => $this->name,
            'primaryKeys' => $this->primaryKeyColumns,
        ];

        foreach ($this->columns as $column) {
            $table['columns'][] = $column->__toArray();
        }
        foreach ($this->indexs as $index) {
            $table['indexs'][] = $index->__toArray();
        }

        return $table;
    }

    public function toSql()
    {
        if ($this->changeType === self::NO_CHANGE) {
            return '';
        }

        switch ($this->changeType) {
            case self::IS_NEW:
                $createTableSql = "CREATE TABLE {$this->name} (";
                foreach ($this->columns as $column) {
                    $createTableSql .= "{$column->getName()} {$column->getFullType()}, ";
                }
                $primaryKeyColumns = implode(',', $this->primaryKeyColumns);
                $createTableSql    .= ") PRIMARY KEY({$primaryKeyColumns})";

                return $createTableSql;
            case self::IS_MODIFIED:
                return ""; // no implement for change table name
            case self::TO_DELETE:
                return "DROP TABLE {$this->name}";
            default:
                return '';
        }
    }

    public function compareColumn(Column $column)
    {
        foreach ($this->columns as $col) {
            if ($col->getName() == $column->getName()) {
                if ($col->getType() == $column->getType()) {
                    return self::NO_CHANGE;
                }
                else {
                    return self::IS_MODIFIED;
                }
            }
        }

        return self::IS_NEW;
    }

    public function hasColumn(Column $column)
    {
        if (!empty($this->columnMap)) {
            return in_array($column->getName(), $this->columnMap);
        }

        foreach ($this->columns as $col) {
            $this->columnMap[] = $col->getName();
        }

        return in_array($column->getName(), $this->columnMap);
    }

    public function compareIndex(Index $index)
    {
        foreach ($this->indexs as $idx) {
            if ($idx->getName() == $index->getName()) {
                return self::NO_CHANGE;
            }
        }

        return self::IS_NEW;
    }

    public function hasIndex(Index $index)
    {
        if (!empty($this->indexMap)) {
            return in_array($index->getName(), $this->indexMap);
        }

        foreach ($this->indexs as $idx) {
            $this->indexMap[] = $idx->getName();
        }

        return in_array($index->getName(), $this->indexMap);
    }

}
