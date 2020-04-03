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

}
