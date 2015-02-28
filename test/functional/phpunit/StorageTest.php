<?php

class StorageTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;
    /** @var  \Mongotd\Mongotd */
    protected $mongotd;

    public function setUp(){
        $this->conn = new \Mongotd\Connection('localhost', 'test', 'test');
        $this->mongotd = new \Mongotd\Mongotd($this->conn);
        $this->conn->dropDb();
    }

    public function test_StoreGaugeValue_RetrievesSameValue(){
        $someResolution = \Mongotd\Resolution::FIFTEEEN_MINUTES;
        $someAggregation = \Mongotd\Aggregation::SUM;
        $inserter = $this->mongotd->getInserter($someResolution);
        $retriever = $this->mongotd->getRetriever();
        $someSid = 1;
        $someNid = 1;
        $someVal = 545;
        $someDateStr = '2015-02-18 15:00:00';
        $someDatetime = new \DateTime($someDateStr);
        $incrementalIsFalse = false;
        $expectedValsByDate = array(
            $someDateStr => $someVal
        );

        $inserter->add($someSid, $someNid, $someDatetime, $someVal, $incrementalIsFalse);
        $inserter->insert();

        $valsByDate = $retriever->get($someSid, $someNid, $someDatetime, $someDatetime, $someResolution, $someAggregation);

        $this->assertTrue($valsByDate === $expectedValsByDate);
    }

    public function test_StoreTwoIncrementalValues_RetrievesDifference(){
        $resolution15min = \Mongotd\Resolution::FIFTEEEN_MINUTES;
        $someAggregation = \Mongotd\Aggregation::SUM;
        $inserter = $this->mongotd->getInserter($resolution15min);
        $retriever = $this->mongotd->getRetriever();
        $someSid = 1;
        $someNid = 1;
        $someValue1 = 600;
        $someValue2 = 700;
        $expectedDifference = $someValue2 - $someValue1;
        $firstDatetime = new \DateTime('2015-02-18 15:00:00');
        $secondDateStr15MinAfterFirst = '2015-02-18 15:15:00';
        $secondDatetime15MinAfterFirst = new \DateTime($secondDateStr15MinAfterFirst);
        $incrementalIsTrue = true;
        $expectedValsByDate = array(
            $secondDateStr15MinAfterFirst => $expectedDifference
        );

        $inserter->add($someSid, $someNid, $firstDatetime, $someValue1, $incrementalIsTrue);
        $inserter->insert();
        $inserter->add($someSid, $someNid, $secondDatetime15MinAfterFirst, $someValue2, $incrementalIsTrue);
        $inserter->insert();

        $valsByDate = $retriever->get($someSid, $someNid, $secondDatetime15MinAfterFirst, $secondDatetime15MinAfterFirst, $resolution15min, $someAggregation);

        $this->assertTrue($valsByDate === $expectedValsByDate);
    }
}