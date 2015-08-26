<?php

use Mongotd\Anomaly;
use \Mongotd\Connection;
use Mongotd\CounterValue;
use \Mongotd\Mongotd;
use \Mongotd\Resolution;
use \Mongotd\Aggregation;

class RetrievalTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;
    /** @var  \Mongotd\Mongotd */
    protected $mongotd;

    public function setUp(){
        $this->conn = new Connection('localhost', 'test', 'test');
        $this->mongotd = new Mongotd($this->conn);
        $this->conn->db()->drop();
    }

    public function test_StoreAnomaly_RetrieveGroupedAndSortedByNofAnomalies(){
        /* This tries to test something that's a bit difficult to test.
         * The anomalies should be retrieved in sorted order from highest number of anomalies to lowest,
         * and contain all the anomalies for each group.
         *
         * We don't test limit and minCount here
         */
        $start = new DateTime('-5 minutes');
        $end = new DateTime('now');

        // Group 1
        $this->conn->col('anomalies')->insert(array('nid' => '1', 'sid' => '1', 'predicted' => 30, 'actual' => 70, 'mongodate' => new MongoDate($start->getTimestamp())));
        $this->conn->col('anomalies')->insert(array('nid' => '1', 'sid' => '1', 'predicted' => 40, 'actual' => 80, 'mongodate' => new MongoDate($end->getTimestamp())));
        $this->conn->col('anomalies')->insert(array('nid' => '1', 'sid' => '1', 'predicted' => 45, 'actual' => 85, 'mongodate' => new MongoDate($end->getTimestamp())));

        // Group 2
        $this->conn->col('anomalies')->insert(array('nid' => '2', 'sid' => '1', 'predicted' => 50, 'actual' => 90, 'mongodate' => new MongoDate($start->getTimestamp())));

        // Group 3
        $this->conn->col('anomalies')->insert(array('nid' => '2', 'sid' => '2', 'predicted' => 60, 'actual' => 100, 'mongodate' => new MongoDate($start->getTimestamp())));
        $this->conn->col('anomalies')->insert(array('nid' => '2', 'sid' => '2', 'predicted' => 60, 'actual' => 100, 'mongodate' => new MongoDate($end->getTimestamp())));

        $retriever = $this->mongotd->getRetriever();
        $res = $retriever->getAnomalies($start, $end, array('1','2'), array('1','2'), 1, 3);

        $this->assertEquals(3, count($res), 'Expected number of groups were wrong');
        $this->assertEquals(3, count($res[0]['anomalies']), 'Expected number of anomalies in grp1 were wrong');
        $this->assertEquals(2, count($res[1]['anomalies']), 'Expected number of anomalies in grp3 were wrong');
        $this->assertEquals(1, count($res[2]['anomalies']), 'Expected number of anomalies in grp2 were wrong');
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

        $inserter = $this->mongotd->getInserter();
        $inserter->setInterval($someResolution);
        $retriever = $this->mongotd->getRetriever();

        $expectedValsByDate = array(
            $someDateString => $expectedSum
        );

        $inserter->add($sid1, $someNid, $datetime, $valSid1, $isIncremental);
        $inserter->add($sid2, $someNid, $datetime, $valSid2, $isIncremental);
        $inserter->insert();

        $valsByTimestamp = $retriever->get(
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
        $valsByDate = $retriever->convertToDateStringKeys($valsByTimestamp, $datetime->getTimezone());

        $msg = 'Formula test';
        $msg .= "\nExpected\n";
        $msg .= json_encode($expectedValsByDate, JSON_PRETTY_PRINT);
        $msg .= "\nActual:\n";
        $msg .= json_encode($valsByDate, JSON_PRETTY_PRINT);

        $this->assertTrue($valsByDate === $expectedValsByDate, $msg);
    }

    public function test_CreateAnomalyStateArray_ResolutionFiveMin_CorrectlyGenerated(){
        $retriever = $this->mongotd->getRetriever();
        $anomalies = array(
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:01:23'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:03:23'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:06:15'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:07:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:09:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:15:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:16:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:19:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:20:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:24:59'), 1), 1),
        );
        $expectedStates = array(
            '2015-02-20 00:00:00' => 1,
            '2015-02-20 00:05:00' => 1,
            '2015-02-20 00:10:00' => 0,
            '2015-02-20 00:15:00' => 1,
            '2015-02-20 00:20:00' => 1,
            '2015-02-20 00:25:00' => 0,
        );

        $start = new DateTime('2015-02-20 00:00:00');
        $end = new DateTime('2015-02-20 00:29:59');

        $statesByTimestamp = $retriever->getAnomalyStates($anomalies, $start, $end, Resolution::FIVE_MINUTES);
        $statesByDate = $retriever->convertToDateStringKeys($statesByTimestamp, $start->getTimezone());

        $this->assertTrue($statesByDate === $expectedStates);
    }
}