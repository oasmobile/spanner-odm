<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;

/**
 * Class Index
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Index
{
    private $name;

    /**
     * @return mixed
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param  mixed  $name
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
            'name' => $this->name,
        ];
    }

}
