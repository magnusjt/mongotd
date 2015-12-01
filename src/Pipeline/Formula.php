<?php namespace Mongotd\Pipeline;

use Mongotd\Kpi\AstEvaluator;

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