<?php

use \Mongotd\Connection;
use Mongotd\CounterValue;
use Mongotd\Logger;
use \Mongotd\Mongotd;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;
use \Mongotd\Resolution;
use \Mongotd\Aggregation;
use Mongotd\Storage;

class StorageTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;


    public function setUp(){
        $this->conn = new Connection('127.0.0.1', 'test', 'test');
        $this->conn->db()->drop();
    }

    public function test_StoreGaugeValue_RetrievesSameValue(){
        $someResolution = Resolution::FIFTEEEN_MINUTES;
        $someAggregation = Aggregation::SUM;
        $storage = new Storage();
        $storage->setDefaultMiddleware($this->conn, new Logger(null), $someResolution);
        $pipelineFactory = new Factory($this->conn);
        $pipeline = new Pipeline();
        $someSid = 1;
        $someNid = 1;
        $someVal = 545;
        $someDateStr = '2015-02-18 15:00:00';
        $someDatetime = new DateTime($someDateStr);
        $incrementalIsFalse = false;
        $expectedValsByDate = array(
            $someDateStr => $someVal
        );

        $storage->store([
            new CounterValue($someSid, $someNid, $someDatetime, $someVal, $incrementalIsFalse)
        ]);

        $sequence = $pipelineFactory->createMultiAction($someSid, $someNid, $someDatetime, $someDatetime, $someResolution, $someAggregation);
        $sequence[] = new ConvertToDateStringKeys();
        $valsByDate = $pipeline->run($sequence);

        $this->assertEquals($expectedValsByDate, $valsByDate);
    }

    public function test_StoreTwoIncrementalValues_RetrievesDifference(){
        $resolution15min = Resolution::FIFTEEEN_MINUTES;
        $someAggregation = Aggregation::SUM;
        $storage = new Storage();
        $storage->setDefaultMiddleware($this->conn, new Logger(null), $resolution15min);
        $pipelineFactory = new Factory($this->conn);
        $pipeline = new Pipeline();
        $someSid = 1;
        $someNid = 1;
        $someValue1 = 600;
        $someValue2 = 700;
        $expectedDifference = $someValue2 - $someValue1;
        $firstDatetime = new DateTime('2015-02-18 15:00:00');
        $secondDateStr15MinAfterFirst = '2015-02-18 15:15:00';
        $secondDatetime15MinAfterFirst = new DateTime($secondDateStr15MinAfterFirst);
        $incrementalIsTrue = true;
        $expectedValsByDate = array(
            $secondDateStr15MinAfterFirst => $expectedDifference
        );

        $storage->store([
            new CounterValue($someSid, $someNid, $firstDatetime, $someValue1, $incrementalIsTrue)
        ]);
        $storage->store([
            new CounterValue($someSid, $someNid, $secondDatetime15MinAfterFirst, $someValue2, $incrementalIsTrue)
        ]);

        $sequence = $pipelineFactory->createMultiAction(
            $someSid, $someNid, $secondDatetime15MinAfterFirst, $secondDatetime15MinAfterFirst, $resolution15min, $someAggregation
        );
        $sequence[] = new ConvertToDateStringKeys();
        $valsByDate = $pipeline->run($sequence);

        $this->assertEquals($expectedValsByDate, $valsByDate);
    }
}