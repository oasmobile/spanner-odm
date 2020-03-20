<?php

namespace Oasis\Mlib\ODM\Spanner\Ut;

use Oasis\Mlib\ODM\Dynamodb\Exceptions\DataConsistencyException;
use Oasis\Mlib\ODM\Dynamodb\ItemManager;
use Oasis\Mlib\ODM\Spanner\Driver\SpannerDbConnection;
use PHPUnit\Framework\TestCase;

class ItemManagerTest extends TestCase
{
    /** @var  ItemManager */
    protected $itemManager;
    /** @var  ItemManager */
    protected $itemManager2;
    
    protected function setUp()
    {
        parent::setUp();
        $this->itemManager  = new ItemManager(
            new SpannerDbConnection(UTConfig::$dbConfig), UTConfig::$tablePrefix, __DIR__ . "/cache", true
        );
        $this->itemManager2 = new ItemManager(
            new SpannerDbConnection(UTConfig::$dbConfig), UTConfig::$tablePrefix, __DIR__ . "/cache", true
        );
    }
    
    public function testPersistAndGet()
    {
        $id   = mt_rand(1000, PHP_INT_MAX);
        $user = new User();
        $user->setId($id);
        $user->setName('Alice');
        $user->setAge(12);
        $user->setWage(2000);
        $user->setAlias('TestUser');
        $this->itemManager->persist($user);
        $this->itemManager->flush();
        
        /** @var User $user2 */
        $user2 = $this->itemManager->get(User::class, ['id' => $id], true);
        
        $this->assertEquals($user, $user2); // user object will be reused when same primary keys are used
        $this->assertEquals('Alice', $user2->getName());
        
        return $id;
    }
    
    /**
     * @depends testPersistAndGet
     *
     * @param $id
     */
    public function testDoublePersist($id)
    {
        $id2  = $id + 1;
        $user = new User();
        $user->setId($id2);
        $user->setName('Howard');
        $this->itemManager->persist($user);
        
        /** @var User $user2 */
        $user2 = $this->itemManager->get(User::class, ['id' => $id2]);
        
        $this->assertEquals($user, $user2); // user object will be reused when same primary keys are used
        $this->assertEquals('Howard', $user2->getName());
        
        /** @var User $user3 */
        $user3 = $this->itemManager->get(User::class, ['id' => $id2], true);
        
        $this->assertNull($user3);
    }
    
    /**
     * @depends testPersistAndGet
     *
     * @param $id
     * @return string
     */
    public function testEdit($id)
    {
        /** @var User $user */
        $user = $this->itemManager->get(User::class, ['id' => $id]);
        $this->assertInstanceOf(User::class, $user);
        $this->assertNotEquals('John', $user->getName());
        $user->setName('John');
        $user->haha = 22;
        $this->itemManager->flush();
        
        $this->itemManager->clear();
        /** @var User $user2 */
        $user2 = $this->itemManager->get(User::class, ['id' => $id]);
        
        $this->assertInstanceOf(User::class, $user2);
        $this->assertTrue($user !== $user2);
        $this->assertEquals('John', $user2->getName());
        
        return $id;
    }
    
    /**
     * @depends testEdit
     *
     * @param $id
     */
    public function testCASEnabled($id)
    {
        /** @var User $user */
        $user = $this->itemManager->get(User::class, ['id' => $id]);
        /** @var User $user2 */
        $user2 = $this->itemManager2->get(User::class, ['id' => $id]);
        
        $user->setName('Chris');
        $this->itemManager->flush();
        
        $user2->setName('Michael');
        self::expectException(DataConsistencyException::class);
        $this->itemManager2->flush();
    }

}
