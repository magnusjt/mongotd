<?php

use \Mongotd\Connection;
use \Mongotd\Mongotd;

class AggregationTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;
    /** @var  \Mongotd\Mongotd */
    protected $mongotd;

    public function setUp(){
        $this->conn = Connection::fromParameters('localhost', 'test', 'test');
        $this->mongotd = new Mongotd($this->conn);
        $this->conn->dropDb();
    }

    public function test_SumAggregateDayOfValues_RetrievesSumOfValues(){
        $retrieveAtDayResolution = \Mongotd\Resolution::DAY;
        $insertAtHourResolution = \Mongotd\Resolution::HOUR;
        $sumAggregation = \Mongotd\Aggregation::SUM;
        $inserter = $this->mongotd->getInserter();
        $inserter->setInterval($insertAtHourResolution);
        $retriever = $this->mongotd->getRetriever();
        $incrementalIsFalse = false;
        $someSid = 1;
        $someNid = 1;
        $someValPerHour = 100;
        $hoursInDay = 24;
        $expectedSum = $someValPerHour * $hoursInDay;
        $startDateStr = '2015-02-28 00:00:00';
        $start = new \DateTime($startDateStr);
        $end = clone $start;
        $end->add(DateInterval::createFromDateString('1 day'));
        $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString('1 hour'), $end);
        $expectedValsByDate = array(
            $startDateStr => $expectedSum
        );

        foreach($dateperiod as $datetime){
            $inserter->add($someSid, $someNid, $datetime, $someValPerHour, $incrementalIsFalse);
        }

        $inserter->insert();
        $valsByDate = $retriever->get($someSid, $someNid, $start, $start, $retrieveAtDayResolution, $sumAggregation);

        $this->assertTrue($valsByDate === $expectedValsByDate);
    }
}