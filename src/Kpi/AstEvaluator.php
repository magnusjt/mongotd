<?php namespace Mongotd\Kpi;

class AstEvaluator{
    private $variableEvaluator = NULL;
    private $padding = false;

    /** @var  OperatorEvaluator */
    private $operatorEvaluator;

    public function __construct(){
        $this->operatorEvaluator = new OperatorEvaluator();
    }

    /**
     * @param $variableEvaluator \Closure
     *
     * This callback function is used to get the values for a variable
     */
    public function setVariableEvaluatorCallback($variableEvaluator){
        $this->variableEvaluator = $variableEvaluator;
    }

    /**
     * @param $padding mixed
     *
     * The padding value is used in order to determine when a variable value is missing.
     */
    public function setPaddingValue($padding){
        $this->padding = $padding;
    }

    public function evaluate($node){
        if($node instanceof NodeNumber){
            return $node->num;
        }elseif($node instanceof NodeBinaryOperator){
            return $this->binop($this->evaluate($node->left), $this->evaluate($node->right), $node->op);
        }elseif($node instanceof NodeUnaryOperator){
            return $this->unop($this->evaluate($node->operand), $node->op);
        }else if($node instanceof NodeVariable){
            return $this->variable($node->options);
        }else{
            throw new \Exception('Unknown node in AST');
        }
    }

    private function binop($left, $right, $op){
        return $this->operatorEvaluator->evaluate($left, $right, $op, $this->padding);
    }

    private function unop($operand, $op){
        if($op == Operator::plus){
            return $operand;
        }elseif($op == Operator::minus){
            return $this->operatorEvaluator->evaluate(-1, $operand, '*', $this->padding);
        }else{
            throw new \Exception('Invalid unary operator ' . $op);
        }
    }

    private function variable($options){
        return call_user_func($this->variableEvaluator, $options);
    }
}