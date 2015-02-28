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
        $someDatetime = new \DateTime('2015-02-18 15:00:00');
        $incrementalIsFalse = false;

        $inserter->add($someSid, $someNid, $someDatetime, $someVal, $incrementalIsFalse);
        $inserter->insert();

        $valsByDate = $retriever->get($someSid, $someNid, $someDatetime, $someDatetime, $someResolution, $someAggregation);

        $this->assertTrue(isset($valsByDate[$someDatetime->format('Y-m-d H:i:s')]), 'Inserted value wasn\'t found again at the same datetime');
        $this->assertEquals($someVal, $valsByDate[$someDatetime->format('Y-m-d H:i:s')], 'Inserted value was not equal to retrieved value');
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
        $secondDatetime15MinAfterFirst = new \DateTime('2015-02-18 15:15:00');
        $incrementalIsTrue = true;

        $inserter->add($someSid, $someNid, $firstDatetime, $someValue1, $incrementalIsTrue);
        $inserter->insert();
        $inserter->add($someSid, $someNid, $secondDatetime15MinAfterFirst, $someValue2, $incrementalIsTrue);
        $inserter->insert();

        $valsByDate = $retriever->get($someSid, $someNid, $secondDatetime15MinAfterFirst, $secondDatetime15MinAfterFirst, $resolution15min, $someAggregation);

        $this->assertTrue(isset($valsByDate[$secondDatetime15MinAfterFirst->format('Y-m-d H:i:s')]), 'Inserted value wasn\'t found again at the same datetime');
        $this->assertEquals($expectedDifference, $valsByDate[$secondDatetime15MinAfterFirst->format('Y-m-d H:i:s')], 'Inserted value was not equal to retrieved value');
    }
}