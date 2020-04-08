<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Index
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Index extends ComparableItem
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

    public function __toArray()
    {
        return [
            'name'    => $this->getName(),
            'table'   => $this->tableName,
            'columns' => $this->columns,
        ];
    }

    /**
     * @return string
     */
    public function getName()
    {
        if (empty($this->name) && !empty($this->columns)) {
            $idxName = '';
            foreach ($this->columns as $col) {
                $idxName .= strtolower($col).'_';
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
        $this->name = str_replace("-", '_', $name);

        return $this;
    }

    public function toSql()
    {
        if (empty($this->tableName)) {
            return '';
        }
        if ($this->changeType === self::NO_CHANGE) {
            return '';
        }

        $indexName = $this->getName();
        $columnSql = implode(',', $this->columns);

        switch ($this->changeType) {
            case self::IS_NEW:
                return "CREATE INDEX {$indexName} ON {$this->tableName}({$columnSql})";
            case self::IS_MODIFIED:
                return ""; // no implement for modify index
            case self::TO_DELETE:
                return "DROP INDEX {$indexName}";
            default:
                return '';
        }
    }
}
