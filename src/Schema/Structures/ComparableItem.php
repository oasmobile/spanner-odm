<?php


namespace Oasis\Mlib\ODM\Spanner\Schema\Structures;


abstract class ComparableItem
{
    const NO_CHANGE   = 0;
    const IS_NEW      = 1;
    const IS_MODIFIED = 2;
    const TO_DELETE   = 3;

    /**
     * @var int
     */
    protected $changeType = 0;

    /**
     * @return int
     */
    public function getChangeType()
    {
        return $this->changeType;
    }

    /**
     * @param  int  $changeType
     */
    public function setChangeType($changeType)
    {
        $this->changeType = $changeType;
    }

    abstract public function toSql();

}
