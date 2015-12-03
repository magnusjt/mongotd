<?php

use Mongotd\Kpi\AstEvaluator;
use Mongotd\Kpi\Parser;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\Formula;
use Mongotd\Pipeline\Series;

class FormulaTest extends PHPUnit_Framework_TestCase{
    public function test_AddTwoSeriesAndAConstant_FormulaAction_RetrieveSum(){
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
            $series = $seriesList[$options['sid']];
            return $series->vals;
        });

        $convert = new ConvertToDateStringKeys();
        $action = new Formula($ast, $astEvaluator);
        $output = $action->run();
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }
}