<?php

use \Mongotd\Connection;
use \Mongotd\Mongotd;
use \Mongotd\Resolution;
use \Mongotd\Aggregation;

class AggregationTest extends PHPUnit_Framework_TestCase{
    /** @var  \Mongotd\Connection */
    protected $conn;
    /** @var  \Mongotd\Mongotd */
    protected $mongotd;

    public function setUp(){
        $this->conn = new Connection('localhost', 'test', 'test');
        $this->mongotd = new Mongotd($this->conn);
        $this->conn->db()->drop();
    }

    public function aggregationTestProvider(){
        return array(
            array(
                array(
                    'description' => 'One day, insert hourly, retrieve daily sum',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::HOUR,
                    'aggregation' => Aggregation::SUM,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 23:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1*24
                    )
                )
            ),
            array(
                array(
                    'description' => 'One day, insert hourly, retrieve daily avg',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::HOUR,
                    'aggregation' => Aggregation::AVG,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 23:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1
                    )
                )
            ),
            array(
                array(
                    'description' => 'One day, insert hourly, retrieve daily max',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::HOUR,
                    'aggregation' => Aggregation::MAX,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 23:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1
                    )
                )
            ),
            array(
                array(
                    'description' => 'One day, insert hourly, retrieve daily min',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::HOUR,
                    'aggregation' => Aggregation::MIN,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 23:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1
                    )
                )
            ),
            array(
                array(
                    'description' => 'Two days, insert five min, retrieve daily sum',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::FIVE_MINUTES,
                    'aggregation' => Aggregation::SUM,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-23 00:00:00',
                    'end' => '2015-02-24 23:59:59',
                    'expected' => array(
                        '2015-02-23 00:00:00' => 1*24*12,
                        '2015-02-24 00:00:00' => 1*24*12
                    )
                )
            ),
            array(
                array(
                    'description' => 'One hour, insert five min, retrieve hourly sum',
                    'timezone' => 'Europe/Oslo',
                    'retrieveResolution' => Resolution::HOUR,
                    'insertResolution' => Resolution::FIVE_MINUTES,
                    'aggregation' => Aggregation::SUM,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 00:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1*12
                    )
                )
            ),
            array(
                array(
                    'description' => 'One day, insert hourly, retrieve daily sum, timezone America/New_York',
                    'timezone' => 'America/New_York',
                    'retrieveResolution' => Resolution::DAY,
                    'insertResolution' => Resolution::HOUR,
                    'aggregation' => Aggregation::SUM,
                    'nid' => 1,
                    'sid' => 1,
                    'val' => 1,
                    'incremental' => false,
                    'start' => '2015-02-28 00:00:00',
                    'end' => '2015-02-28 23:59:59',
                    'expected' => array(
                        '2015-02-28 00:00:00' => 1*24
                    )
                )
            ),

        );
    }

    /**
     * @dataProvider aggregationTestProvider
     *
     * @param $config
     */
    public function test_Aggregate_ExpectedEqualsRetrieved($config){
        $timezone = new DateTimeZone($config['timezone']);
        $start = new DateTime($config['start'], $timezone);
        $end = new DateTime($config['end'], $timezone);
        $inserter = $this->mongotd->getInserter();
        $inserter->setInterval($config['insertResolution']);
        $retriever = $this->mongotd->getRetriever();

        $dateperiod = new DatePeriod($start, DateInterval::createFromDateString($config['insertResolution'] . ' seconds'), $end);
        foreach($dateperiod as $datetime){
            $inserter->add($config['sid'], $config['nid'], $datetime, $config['val'], $config['incremental']);
        }
        $inserter->insert();

        $valsByDate = $retriever->get($config['sid'], $config['nid'], $start, $end, $config['retrieveResolution'], $config['aggregation'], 'x');
        $msg = $config['description'];
        $msg .= "\nExpected\n";
        $msg .= json_encode($config['expected'], JSON_PRETTY_PRINT);
        $msg .= "\nActual:\n";
        $msg .= json_encode($valsByDate, JSON_PRETTY_PRINT);
        $this->assertTrue($valsByDate === $config['expected'], $msg);
    }
}