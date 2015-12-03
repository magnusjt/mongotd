<?php

use Mongotd\Connection;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;

class AnomalyRetrievalTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;

    public function setUp(){
        $this->conn = new Connection('127.0.0.1', 'test', 'test');
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
        $this->conn->col('anomalies')->insert(['nid' => '1', 'sid' => '1', 'predicted' => 30, 'actual' => 70, 'mongodate' => new MongoDate($start->getTimestamp())]);
        $this->conn->col('anomalies')->insert(['nid' => '1', 'sid' => '1', 'predicted' => 40, 'actual' => 80, 'mongodate' => new MongoDate($end->getTimestamp())]);
        $this->conn->col('anomalies')->insert(['nid' => '1', 'sid' => '1', 'predicted' => 45, 'actual' => 85, 'mongodate' => new MongoDate($end->getTimestamp())]);

        // Group 2
        $this->conn->col('anomalies')->insert(['nid' => '2', 'sid' => '1', 'predicted' => 50, 'actual' => 90, 'mongodate' => new MongoDate($start->getTimestamp())]);

        // Group 3
        $this->conn->col('anomalies')->insert(['nid' => '2', 'sid' => '2', 'predicted' => 60, 'actual' => 100, 'mongodate' => new MongoDate($start->getTimestamp())]);
        $this->conn->col('anomalies')->insert(['nid' => '2', 'sid' => '2', 'predicted' => 60, 'actual' => 100, 'mongodate' => new MongoDate($end->getTimestamp())]);

        $pipeline = new Pipeline();
        $pipelineFactory = new Factory($this->conn);
        $sequence = $pipelineFactory->createAnomalyAction($start, $end, ['1','2'], ['1','2'], 1, 3);

        $res = $pipeline->run($sequence);

        $this->assertEquals(3, count($res), 'Expected number of groups were wrong');
        $this->assertEquals(3, count($res[0]['anomalies']), 'Expected number of anomalies in grp1 were wrong');
        $this->assertEquals(2, count($res[1]['anomalies']), 'Expected number of anomalies in grp3 were wrong');
        $this->assertEquals(1, count($res[2]['anomalies']), 'Expected number of anomalies in grp2 were wrong');
    }
}