<?php

use \Mongotd\KpiParser;
use \Mongotd\AstEvaluator;

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
            array('-(5 - 10)', 5),
            array('1*1*1*1/1*1*1*1*1*(1*1*1)*1*1*1*(1*1*1*1)', 1),
            array('2*2/2*2', 1),
            array('---5', -5)
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
            array('5 + [nid=5,sid=2,agg=sum,val=5]', 10),
            array('5 + [nid=5,sid=2,agg=sum,val=5]*5', 30),
            array('5 + [val=5]*[val=10]', 55),
            array('5 + [val=1]+[val=2]*([val=5]+[val=10])', 36),
            array('-[val=1]', -1),
            array('100 * [val=50]/[val=100]', 50),
        );
    }

    /**
     * @dataProvider variableArithmeticProvider
     *
     * @param $expression     string
     * @param $expectedResult number
     */
    public function test_variableArithmetic_EvaluateAst_MatchExpected($expression, $expectedResult){
        $parser = new KpiParser();
        $evaluator = new AstEvaluator();
        $evaluator->setVariableEvaluatorCallback(function($options){
            return $options['val'];
        });

        $node = $parser->parse($expression);
        $result = $evaluator->evaluate($node);

        $this->assertEquals($expectedResult, $result, $expression . ' was equal to ' . $result);
    }

    // Remember that variables are predefined in the test below
    public function arrayArithmeticProvider(){
        return array(
            array('1 + [name=value]', array(2,3,4)),
            array('1 - [name=value]', array(0,-1,-2)),
            array('2 * [name=value]', array(2,4,6)),
            array('6 / [name=value]', array(6,3,2)),
            array('[name=value]*[name=value]', array(1,4,9)),
            array('[name=value]/[name=value]', array(1,1,1)),
            array('[name=value]-[name=value]', array(0,0,0)),
        );
    }

    /**
     * @dataProvider arrayArithmeticProvider
     *
     * @param $expression     string
     * @param $expectedResult number
     */
    public function test_arrayArithmetic_EvaluateAst_MatchExpected($expression, $expectedResult){
        $parser = new KpiParser();
        $evaluator = new AstEvaluator();
        $evaluator->setVariableEvaluatorCallback(function($options){
            return array(1,2,3);
        });

        $node = $parser->parse($expression);
        $result = $evaluator->evaluate($node);

        $this->assertEquals(implode(',', $expectedResult), implode(',', $result), $expression . ' was equal to ' . implode(',', $result));

    }

    public function syntaxErrorArithmeticProvider(){
        return array(
            array('5 +/ 5', 'Mangled operators'),
            array('5 * (5+5', 'Unbalanced parentheses1'),
            array('5 * 5+5)', 'Unbalanced parentheses2'),
            array('5 * (5+5))', 'Unbalanced parentheses3'),
            array('[val=5', 'Unbalanced brackets1'),
            array('val=5]', 'Unbalanced brackets2'),
            array('[blarg]', 'Variable with no assignment'),
            array('[]', 'Empty variable'),
            array('{2blarg=5]', 'Variable with identifier beginning with a number'),
            array('0,5', 'Float using comma instead of dot'),
            array('[val=hello hello]', 'Variable with value containing a space'),
        );
    }

    /**
     * @dataProvider syntaxErrorArithmeticProvider
     *
     * @param $expression string
     * @param $description string
     */
    public function test_syntaxErrorArithmetic_EvaluateAst_ThrowsException($expression, $description){
        $parser = new KpiParser();
        $evaluator = new AstEvaluator();
        $evaluator->setVariableEvaluatorCallback(function($options){
            return $options['val'];
        });

        try{
            $parser->parse($expression);
        }catch(\Exception $e){
            return;
        }

        $this->fail('Expected exception, but parsing succeeded for test: ' . $description);
    }
}