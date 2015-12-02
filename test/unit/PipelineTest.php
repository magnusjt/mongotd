<?php

use Mongotd\Aggregation;
use Mongotd\Kpi\AstEvaluator;
use Mongotd\Kpi\Parser;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\Formula;
use Mongotd\Pipeline\Pad;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Pipeline\Find;
use Mongotd\Pipeline\RollupTime;
use Mongotd\Pipeline\Series;
use Mongotd\Resolution;

class PipelineTest extends PHPUnit_Framework_TestCase{
    private $find;

    public function setUp(){
        $this->find = $this->getMockBuilder('Mongotd\Pipeline\Find')->disableOriginalConstructor()->getMock();
    }

    public function rollupTimeAndPadProvider(){
        return [
            [
                [
                    'description' => 'Time including daylight savings shift, insert random vals, retrieve daily sum',
                    'timezone' => 'Europe/Oslo',
                    'resolution' => Resolution::DAY,
                    'aggregation' => Aggregation::SUM,
                    'start' => '2015-10-25 00:00:00',
                    'end' => '2015-10-26 00:00:00',
                    'padding' => false,
                    'vals' => [
                        '2015-10-25 00:00:00' => 100,
                        '2015-10-25 00:05:00' => 100,
                        '2015-10-25 00:10:00' => 100,
                        '2015-10-25 01:00:00' => 200,
                        '2015-10-25 02:00:00' => 300,
                        '2015-10-25 03:00:00' => 400,
                        '2015-10-25 04:00:00' => 500,
                        '2015-10-25 05:00:00' => 600,
                    ],
                    'expected' => [
                        '2015-10-25 00:00:00' => 2300
                    ]
                ]
            ],
            [
                [
                    'description' => 'Time range outside time where values exist, insert random vals, retrieve daily sum and padded',
                    'timezone' => 'Europe/Oslo',
                    'resolution' => Resolution::DAY,
                    'aggregation' => Aggregation::SUM,
                    'start' => '2015-10-26 00:00:00',
                    'end' => '2015-10-28 00:00:00',
                    'padding' => false,
                    'vals' => [
                        '2015-10-26 00:00:00' => 100,
                    ],
                    'expected' => [
                        '2015-10-26 00:00:00' => 100,
                        '2015-10-27 00:00:00' => false
                    ]
                ]
            ],
        ];
    }

    /**
     * @dataProvider rollupTimeAndPadProvider
     *
     * @param $config
     */
    public function test_RollupTimeAndPad($config){
        date_default_timezone_set($config['timezone']);

        $vals = [];
        foreach($config['vals'] as $dateStr => $value){
            $vals[(new DateTime($dateStr))->getTimestamp()] = $value;
        }

        $series = new Series($vals);
        $find = $this->getMockBuilder('Mongotd\Pipeline\Find')->disableOriginalConstructor()->getMock();
        $find->method('run')->willReturn($series);

        $pipeline = new Pipeline();
        $output = $pipeline->run([
            $find,
            new RollupTime($config['resolution'], $config['aggregation'], $config['padding']),
            new Pad($config['resolution'], new DateTime($config['start']), new DateTime($config['end']), $config['padding']),
            new ConvertToDateStringKeys()
        ]);

        $this->assertEquals($config['expected'], $output);
    }
}