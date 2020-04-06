<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Index
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Index
{
    /**
     * @var string
     */
    private $name = '';

    /**
     * @var string
     */
    private $tableName = '';

    /**
     * @var array
     */
    private $columns = [];

    /**
     * @return array
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * @param  array  $columns
     * @return Index
     */
    public function setColumns($columns)
    {
        $this->columns = $columns;

        return $this;
    }

    /**
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * @param  string  $tableName
     */
    public function setTableName($tableName)
    {
        $this->tableName = $tableName;
    }

    /**
     * @return string
     */
    public function getName()
    {
        if (empty($this->name) && !empty($this->columns)) {
            $idxName = '';
            foreach ($this->columns as $col) {
                $idxName .= strtolower($col) . '_';
            }

            return rtrim($idxName, '_');
        }

        return $this->name;
    }

    /**
     * @param  string  $name
     * @return Index
     */
    public function setName($name)
    {
        $this->name = $name;

        return $this;
    }

    public function __toArray()
    {
        return [
            'name'    => $this->getName(),
            'columns' => $this->columns,
        ];
    }

}
