<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Column
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structure
 */
class Column
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
     * @var int
     */
    private $length = 0;

    /**
     *
     * contains type and length which designed for sql generation
     *
     * @var string
     */
    private $fullType = '';

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
     * @return int
     */
    public function getLength()
    {
        return $this->length;
    }

    /**
     * @param  int  $length
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
            'name'   => $this->name,
            'type'   => $this->type,
            'length' => $this->length,
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
                    "T" => "STRING",
                    "F" => "STRING(MAX)",
                ],
            "map"    =>
                [
                    "T" => "STRING",
                    "F" => "STRING(MAX)",
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

}
