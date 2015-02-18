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
        $some_resolution = \Mongotd\Resolution::FIFTEEEN_MINUTES;
        $some_aggregation = \Mongotd\Aggregation::SUM;
        $inserter = $this->mongotd->getInserter($some_resolution);
        $retriever = $this->mongotd->getRetriever();
        $some_sid = 1;
        $some_nid = 1;
        $some_val = 545;
        $some_datetime = new \DateTime('2015-02-18 15:00:00');
        $incremental_is_false = false;

        $inserter->add($some_sid, $some_nid, $some_datetime, $some_val, $incremental_is_false);
        $inserter->insert();

        $vals_by_date = $retriever->get($some_sid, $some_nid, $some_datetime, $some_datetime, $some_resolution, $some_aggregation);

        $this->assertTrue(isset($vals_by_date[$some_datetime->format('Y-m-d H:i:s')]), 'Inserted value wasn\'t found again at the same datetime');
        $this->assertEquals($some_val, $vals_by_date[$some_datetime->format('Y-m-d H:i:s')], 'Inserted value was not equal to retrieved value');
    }
}