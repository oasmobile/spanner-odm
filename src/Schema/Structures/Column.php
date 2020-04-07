<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Column
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structure
 */
class Column extends ComparableItem
{
    /**
     * @var string
     */
    private $name = '';

    /**
     * @var string
     */
    private $type = '';

    /**
     * @var string
     */
    private $length = '';

    /**
     *
     * contains type and length which designed for sql generation
     *
     * @var string
     */
    private $fullType = '';

    /**
     * @var string
     */
    private $tableName = '';

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
    public function getFullType()
    {
        return $this->fullType;
    }

    /**
     * @param  string  $fullType
     */
    public function setFullType($fullType)
    {
        $this->fullType = $fullType;
    }

    /**
     * @return string
     */
    public function getLength()
    {
        return $this->length;
    }

    /**
     * @param  string $length
     * @return Column
     */
    public function setLength($length)
    {
        $this->length = $length;

        return $this;
    }

    /**
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @param  string  $type
     * @return Column
     */
    public function setType($type)
    {
        $this->type = $type;

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
     * @return Column
     */
    public function setName($name)
    {
        $this->name = $name;

        return $this;
    }

    public function __toArray()
    {
        return [
            'name'     => $this->name,
            'table'    => $this->tableName,
            'type'     => $this->type,
            'fullType' => $this->fullType,
            'length'   => $this->length,
        ];
    }

    public function initWithObjectAttribute($attributeName, $attributeType)
    {
        $attMap = $this->getAttributeTypeMapping($attributeType);

        $this->name     = $attributeName;
        $this->type     = $attMap['T'];
        $this->fullType = $attMap['F'];

        return $this;
    }

    protected function getAttributeTypeMapping($attributeType)
    {
        $map = [
            "string" =>
                [
                    "T" => "STRING",
                    "F" => "STRING(MAX)",
                ],
            "number" =>
                [
                    "T" => "INT64",
                    "F" => "INT64",
                ],
            "binary" =>
                [
                    "T" => "BYTES",
                    "F" => "BYTES(MAX)",
                ],
            "bool"   =>
                [
                    "T" => "BOOL",
                    "F" => "BOOL",
                ],
            "null"   =>
                [
                    "T" => "STRING",
                    "F" => "STRING(MAX)",
                ],
            "list"   =>
                [
                    "T" => "ARRAY",
                    "F" => "ARRAY<STRING(MAX)>",
                ],
            "map"    =>
                [
                    "T" => "ARRAY",
                    "F" => "ARRAY<STRING(MAX)>",
                ],

        ];

        if (isset($map[$attributeType])) {
            return $map[$attributeType];
        }
        else {
            return [
                "T" => "STRING",
                "F" => "STRING(MAX)",
            ];
        }
    }


    public function toSql()
    {
        if (empty($this->tableName)) {
            return '';
        }
        if ($this->changeType === self::NO_CHANGE) {
            return '';
        }

        switch ($this->changeType) {
            case self::IS_NEW:
                return "ALTER TABLE {$this->tableName} ADD COLUMN {$this->name} {$this->fullType}";
            case self::IS_MODIFIED:
                return "ALTER TABLE {$this->tableName} ALTER COLUMN {$this->name} {$this->fullType}";
            case self::TO_DELETE:
                return "ALTER TABLE {$this->tableName} DROP COLUMN {$this->name}";
            default:
                return '';
        }
    }
}
