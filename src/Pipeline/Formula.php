<?php namespace Mongotd\Pipeline;

use Mongotd\Kpi\AstEvaluator;

/**
 * Entry pipe which finds values based on a formula.
 * Everything to do with the formula itself, and the
 * way it gets its data is outside of this class.
 *
 * @see Mongotd\Kpi\Parser
 */
class Formula{
    public $ast;
    public $astEvaluator;

    public function __construct($ast, AstEvaluator $astEvaluator){
        $this->ast = $ast;
        $this->astEvaluator = $astEvaluator;
    }

    public function run(){
        return new Series($this->astEvaluator->evaluate($this->ast));
    }
}