<?php

namespace Oasis\Mlib\ODM\Spanner\Ut;

use Symfony\Component\Yaml\Yaml;

class UTConfig
{
    public static $dbConfig = [];
    public static $tablePrefix    = 'odm-test-';
    
    public static function load()
    {
        $file = __DIR__ . "/ut.yml";
        $yml  = Yaml::parse(file_get_contents($file));
        
        self::$dbConfig = $yml['dynamodb'];
        self::$tablePrefix    = $yml['prefix'];
    }
}
