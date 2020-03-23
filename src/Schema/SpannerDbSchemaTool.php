<?php
/**
 * Created by PhpStorm.
 * User: xuchang
 * Date: 2020-03-07
 * Time: 14:00
 */


namespace Oasis\Mlib\ODM\Spanner\Schema;


use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;

/**
 * Class SpannerDbSchemaTool
 * @package Oasis\Mlib\ODM\Dynamodb\DBAL\Schema
 */
class SpannerDbSchemaTool extends AbstractSchemaTool
{

    public function createSchema($skipExisting, $dryRun)
    {
        // TODO: Implement createSchema() method.
    }

    public function updateSchema($isDryRun)
    {
        // TODO: Implement updateSchema() method.
    }

    public function dropSchema()
    {
        // TODO: Implement dropSchema() method.
    }
}
