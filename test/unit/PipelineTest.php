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

    public function basicPipelineProvider(){
        return [
            [
                [
                    'description' => 'Time including daylight savings shift, insert random vals, retrieve daily sum',
                    'timezone' => 'Europe/Oslo',
                    'resolution' => Resolution::DAY,
                    'aggregation' => Aggregation::SUM,
                    'start' => '2015-10-25 00:00:00',
                    'end' => '2015-10-26 00:00:00',
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
        ];
    }

    /**
     * @dataProvider basicPipelineProvider
     *
     * @param $config
     */
    public function test_BasicPipeline($config){
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
            new RollupTime($config['resolution'], $config['aggregation'], false),
            new Pad($config['resolution'], new DateTime($config['start']), new DateTime($config['end']), false),
            new ConvertToDateStringKeys()
        ]);

        $this->assertEquals($config['expected'], $output);
    }

    public function test_FormulaPipeline(){
        $formula = '[sid=0] + [sid=1] + 5';
        $vals1 = [
            (new DateTime('2015-12-05 00:00:00'))->getTimestamp() => 100,
            (new DateTime('2015-12-05 01:00:00'))->getTimestamp() => 200,
        ];
        $vals2 = [
            (new DateTime('2015-12-05 00:00:00'))->getTimestamp() => 1000,
            (new DateTime('2015-12-05 01:00:00'))->getTimestamp() => 2000,
        ];
        $expected = [
            '2015-12-05 00:00:00' => 1105,
            '2015-12-05 01:00:00' => 2205,
        ];

        $seriesList = [];
        $seriesList[] = new Series($vals1);
        $seriesList[] = new Series($vals2);

        $parser = new Parser();
        $ast = $parser->parse($formula);

        $astEvaluator = new AstEvaluator();
        $astEvaluator->setPaddingValue(false);
        $astEvaluator->setVariableEvaluatorCallback(function($options) use($seriesList){
            $find = $this->getMockBuilder('Mongotd\Pipeline\Find')->disableOriginalConstructor()->getMock();

            $series = $seriesList[$options['sid']];
            $find->method('run')->willReturn($series);

            $pipeline = new Pipeline();
            return $pipeline->run([
                $find
            ])->vals;
        });

        $pipeline = new Pipeline();
        $output = $pipeline->run([
            new Formula($ast, $astEvaluator),
            new ConvertToDateStringKeys()
        ]);

        $this->assertEquals($expected, $output);
    }
}