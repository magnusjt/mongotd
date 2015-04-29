<?php

use \Mongotd\KpiParser;
use \Mongotd\AstEvaluator;
use \Mongotd\NodeUnaryOperator;
use \Mongotd\NodeBinaryOperator;
use \Mongotd\NodeNumber;
use \Mongotd\NodeVariable;
use \Mongotd\Operator;

class KpiParserTest extends PHPUnit_Framework_TestCase{

    public function basicArithmeticProvider(){
    return array(
        array('2 + 2', 4),
        array('2 + 2*2', 6),
        array('8/4', 2),
        array('-1', -1),
        array('+1', 1),
        array('-2*2', -4),
        array('8*-5', -40),
        array('5*(5+5)', 50),
        array('5*(5/5+5/5)', 10),
        array('5.5+5.5', 11),
    );
}

    /**
     * @dataProvider basicArithmeticProvider
     *
     * @param $expression
     * @param $expectedResult
     */
    public function test_BasicArithmetic_EvaluateAst_MatchExpected($expression, $expectedResult){
        $parser = new KpiParser();
        $evaluator = new AstEvaluator();

        $node = $parser->parse($expression);
        $result = $evaluator->evaluate($node);

        $this->assertEquals($expectedResult, $result, $expression . ' was equal to ' . $result);
    }

    public function variableArithmeticProvider(){
        return array(
            array('5 + [nid=5,sid=2,agg=sum]', 10),
            array('5 + [nid=5,sid=2,agg=sum]*5', 30)
        );
    }

    /**
     * @dataProvider variableArithmeticProvider
     *
     * @param $expression
     * @param $expectedResult
     */
    public function test_variableArithmetic_EvaluateAst_MatchExpected($expression, $expectedResult){
        $parser = new KpiParser();
        $evaluator = new AstEvaluator();
        $evaluator->setVariableEvaluatorCallback(function($options){
            return 5;
        });

        $node = $parser->parse($expression);
        $result = $evaluator->evaluate($node);

        $this->assertEquals($expectedResult, $result, $expression . ' was equal to ' . $result);
    }
}