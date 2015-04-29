<?php
namespace Mongotd;

class KpiParser{
    /** @var $lexer Lexer */
    private $lexer;

    /**
     * @param $str string
     *
     * @throws \Exception
     * @return mixed
     */
    public function parse($str){
        $this->lexer = new Lexer($str);

        $ast = $this->expression();

        if($this->lexer->get_next_token()->kind != Token::end){
            throw new \Exception("KPI Parser Error: Syntax error - End expected");
        }

        return $ast;
    }

    private function expression(){
        $left = $this->factor();
        $token = $this->lexer->get_next_token();

        if($token->kind == Token::plus){
            $operator = Operator::plus;
        }elseif($token->kind == Token::minus){
            $operator = Operator::minus;
        }else{
            $this->lexer->revert();
            return $left;
        }

        $right = $this->expression();
        return new NodeBinaryOperator($left, $right, $operator);
    }

    private function factor(){
        $left = $this->number();
        $token = $this->lexer->get_next_token();

        if($token->kind == Token::multiply){
            $operator = Operator::multiply;
        }else if($token->kind == Token::divide){
            $operator = Operator::divide;
        }else{
            $this->lexer->revert();
            return $left;
        }

        $right = $this->factor();
        return new NodeBinaryOperator($left, $right, $operator);
    }

    private function number(){
        $token = $this->lexer->get_next_token();

        if($token->kind == Token::lparen){
            $node = $this->expression();
            $expected_rparen = $this->lexer->get_next_token();
            if($expected_rparen->kind != Token::rparen){
                throw new \Exception("KPI Parser Error: Syntax error - Unbalanced paranthesis");
            }

            return $node;
        }elseif($token->kind == Token::number){
            return new NodeNumber($token->value);
        }elseif($token->kind == Token::variable){
            return new NodeVariable($token->value);
        }elseif($token->kind == Token::minus){
            return new NodeUnaryOperator($this->factor(), Operator::minus);
        }elseif($token->kind == Token::plus){
            return new NodeUnaryOperator($this->factor(), Operator::plus);
        }else{
            throw new \Exception("KPI Parser Error: Syntax error - Not a number");
        }
    }
}

class AstEvaluator{
    private $variableEvaluator;

    public function setVariableEvaluatorCallback($variableEvaluator){
        $this->variableEvaluator = $variableEvaluator;
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
        if($op == Operator::plus){
            return $left + $right;
        }elseif($op == Operator::minus){
            return $left - $right;
        }elseif($op == Operator::multiply){
            return $left*$right;
        }elseif($op == Operator::divide){
            return $left/$right;
        }else{
            throw new \Exception('Invalid binary operator ' . $op);
        }
    }

    private function unop($operand, $op){
        if($op == Operator::plus){
            return $operand;
        }elseif($op == Operator::minus){
            return -$operand;
        }else{
            throw new \Exception('Invalid unary operator ' . $op);
        }
    }

    private function variable($options){
        return call_user_func($this->variableEvaluator, $options);
    }
}

class Operator{
    const plus = '+';
    const minus = '-';
    const multiply = '*';
    const divide = '/';
}

class NodeVariable{
    public $options;

    public function __construct($options){
        $this->options = $options;
    }
}

class NodeNumber{
    public $num;

    public function __construct($num){
        $this->num = $num;
    }
}

class NodeBinaryOperator{
    public $left;
    public $right;
    public $op;

    public function __construct($left, $right, $op){
        $this->left = $left;
        $this->right = $right;
        $this->op = $op;
    }
}

class NodeUnaryOperator{
    public $operand;
    public $op;

    public function __construct($operand, $op){
        $this->operand = $operand;
        $this->op = $op;
    }
}

class Token{
    const multiply = 0;
    const divide   = 1;
    const plus     = 2;
    const minus    = 3;

    const number   = 4;
    const variable = 5;

    const lparen = 6;
    const rparen = 7;

    const end = 8;

    public $value = NULL;
    public $kind  = NULL;
}

class Lexer{
    /** @var $input string */
    private $input;

    /** @var $return_previous_token bool */
    private $return_previous_token;

    /** @var $previous_token integer */
    private $previous_token;

    /** @var $token Token */
    private $token;

    /** @var $input string */
    function __construct($input){
        $this->input                 = $input;
        $this->return_previous_token = false;
    }

    /** @return Token
     * @throws \Exception
     */
    public function get_next_token(){
        if($this->return_previous_token){
            $this->return_previous_token = false;
            return $this->previous_token;
        }

        $this->token       = new Token;
        $this->input       = ltrim($this->input);
        $last_match_length = 0;
        if($this->input === ''){
            $this->token->kind = Token::end;
        }else{
            $first_char        = $this->input[0];
            $last_match_length = 1;
            if($first_char == "+"){
                $this->token->kind = Token::plus;
            }elseif($first_char == "-"){
                $this->token->kind = Token::minus;
            }elseif($first_char == "*"){
                $this->token->kind = Token::multiply;
            }elseif($first_char == "/"){
                $this->token->kind = Token::divide;
            }elseif(preg_match('/^(\d+(?:\.\d+)?)/', $this->input, $matches) == 1){
                $this->token->kind  = Token::number;
                $this->token->value = $matches[1];
                $last_match_length  = strlen($matches[0]);
            }elseif(preg_match('/^\[([^\[]+)\]/', $this->input, $matches) == 1){
                $this->token->kind  = Token::variable;
                $optionsRaw = explode(',', $matches[1]);
                $options = array();
                foreach($optionsRaw as $option){
                    $arr = explode('=', $option);
                    $name = trim($arr[0]);
                    $val = trim($arr[1]);
                    $options[$name] = $val;
                }

                $this->token->value = $options;
                $last_match_length  = strlen($matches[0]);
            }elseif($first_char == "("){
                $this->token->kind = Token::lparen;
            }elseif($first_char == ")"){
                $this->token->kind = Token::rparen;
            }else{
                throw new \Exception("Uknown token: " . $first_char);
            }
        }

        $this->input = substr($this->input, $last_match_length);
        $this->previous_token = clone $this->token;

        return $this->token;
    }

    public function revert(){
        $this->return_previous_token = true;
    }
}