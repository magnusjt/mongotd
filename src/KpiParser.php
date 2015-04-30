<?php
namespace Mongotd;

# Grammar:
# - Right recursive since we use recursive descent parsing
#   Right recursive means that the productions never yield the symbol itself before anything else (directly or indirectly)
# - S is the start symbol, upper case are terminals
#
# S          -> expr
# INTFLOAT   -> (\d+(?:\.\d+)?)
# ID         -> \w+\d*
# expr       -> factor + expr | factor - expr | factor
# factor     -> number * factor | number / factor | number
# number     -> (expr) | [var] | INTFLOAT | - number | + number
# var        -> assign | assign , var
# assign     -> ID = INTFLOAT | ID = ID


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

class Operator{
    const plus     = '+';
    const minus    = '-';
    const multiply = '*';
    const divide   = '/';
}

class Token{
    const multiply = 0;
    const divide   = 1;
    const plus     = 2;
    const minus    = 3;
    const number   = 4;
    const variable = 5;
    const id       = 6;
    const comma    = 7;
    const equals   = 8;
    const lparen   = 9;
    const rparen   = 10;
    const lbracket = 11;
    const rbracket = 12;
    const end      = 13;

    public $value = NULL;
    public $kind  = NULL;
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
            $node            = $this->expression();
            $expected_rparen = $this->lexer->get_next_token();
            if($expected_rparen->kind != Token::rparen){
                throw new \Exception("KPI Parser Error: Syntax error - Unbalanced paranthesis");
            }

            return $node;
        }elseif($token->kind == Token::lbracket){
            $node = $this->variable();
            $expected_rbracket = $this->lexer->get_next_token();
            if($expected_rbracket->kind != Token::rbracket){
                throw new \Exception("KPI Parser Error: Syntax error - Unbalanced brackets");
            }

            return $node;
        }elseif($token->kind == Token::number){
            return new NodeNumber($token->value);
        }elseif($token->kind == Token::variable){
            return new NodeVariable($token->value);
        }elseif($token->kind == Token::minus){
            return new NodeUnaryOperator($this->number(), Operator::minus);
        }elseif($token->kind == Token::plus){
            return new NodeUnaryOperator($this->number(), Operator::plus);
        }else{
            throw new \Exception("KPI Parser Error: Syntax error - Not a number: " . $token->value);
        }
    }

    private function variable(){
        $options = $this->assignment();
        $token = $this->lexer->get_next_token();

        if($token->kind != Token::comma){
            $this->lexer->revert();
            return new NodeVariable($options);
        }

        $node = $this->variable();
        $node->options = array_merge($node->options, $options);
        return $node;
    }

    private function assignment(){
        $token = $this->lexer->get_next_token();
        if($token->kind != Token::id){
            throw new \Exception('Expected ID, got ' . $token->value);
        }

        $name = $token->value;

        $token = $this->lexer->get_next_token();
        if($token->kind != Token::equals){
            throw new \Exception('Expected equal sign, got ' . $token->value);
        }

        $token = $this->lexer->get_next_token();
        if($token->kind != Token::id and $token->kind != Token::number){
            throw new \Exception('Expected ID or number, got ' . $token->value);
        }

        return array($name => $token->value);
    }
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
            }elseif($first_char == "["){
                $this->token->kind = Token::lbracket;
            }elseif($first_char == "]"){
                $this->token->kind = Token::rbracket;
            }elseif($first_char == "("){
                $this->token->kind = Token::lparen;
            }elseif($first_char == ")"){
                $this->token->kind = Token::rparen;
            }elseif($first_char == ","){
                $this->token->kind = Token::comma;
            }elseif($first_char == "="){
                $this->token->kind = Token::equals;
            }elseif(preg_match('/^(\d+(?:\.\d+)?)/', $this->input, $matches) == 1){
                $this->token->kind  = Token::number;
                $this->token->value = $matches[1];
                $last_match_length  = strlen($matches[0]);
            }elseif(preg_match('/^(\w+\d*)/', $this->input, $matches) == 1){
                $this->token->kind  = Token::id;
                $this->token->value = $matches[1];
                $last_match_length  = strlen($matches[0]);
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