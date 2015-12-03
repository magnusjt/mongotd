<?php

use Mongotd\Connection;
use Mongotd\CounterValue;
use Mongotd\Logger;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Resolution;
use Mongotd\Aggregation;
use Mongotd\Storage;

class FormulaRetrievalTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;

    public function setUp(){
        $this->conn = new Connection('127.0.0.1', 'test', 'test');
        $this->conn->db()->drop();
    }

    public function test_StoreValues_RetrieveAsFormula_FormulaIsCorrectlyCalculated(){
        $formula = '[sid=1,agg=1] + [sid=2,agg=1]';

        $sid1 = 1;
        $sid2 = 2;
        $someNid = 1;
        $valSid1 = 15;
        $valSid2 = 30;
        $expectedSum = $valSid1 + $valSid2;
        $someDateString = '2015-02-18 15:00:00';
        $datetime = new DateTime($someDateString);
        $someFormulaResolution = Resolution::FIVE_MINUTES;
        $someResultResolution = Resolution::FIVE_MINUTES;
        $sumResultAggregation = Aggregation::SUM;
        $someResolution = Resolution::FIVE_MINUTES;
        $isIncremental = false;
        $padding = false;

        $storage = new Storage();
        $storage->setDefaultMiddleware($this->conn, new Logger(null), $someResolution);
        $pipeline = new Pipeline();
        $pipelineFactory = new Factory($this->conn);

        $expected = array(
            $someDateString => $expectedSum
        );

        $cvs = [];
        $cvs[] = new CounterValue($sid1, $someNid, $datetime, $valSid1, $isIncremental);
        $cvs[] = new CounterValue($sid2, $someNid, $datetime, $valSid2, $isIncremental);
        $storage->store($cvs);

        $sequence = $pipelineFactory->createMultiAction(
            $formula,
            $someNid,
            $datetime,
            $datetime,
            $someResultResolution,
            $sumResultAggregation,
            $padding,
            null,
            null,
            null,
            true,
            $someFormulaResolution
        );

        $sequence[] = new ConvertToDateStringKeys();
        $output = $pipeline->run($sequence);

        $this->assertEquals($expected, $output);
    }
}