<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Table
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Table
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
            'name' => $this->name,
        ];

        foreach ($this->columns as $column) {
            $table['columns'][] = $column->__toArray();
        }
        foreach ($this->indexs as $index) {
            $table['indexs'][] = $index->__toArray();
        }

        return $table;
    }


}
