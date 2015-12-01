<?php namespace Mongotd\Kpi;

/* KPI syntax is defined by the grammar below. Here's some examples:
 * 100 * [name=successes] / [name=attempts]
 * [nid=1,sid=2,aggregation=Sum] + [nid=1,sid=3,aggregation=Avg]
 *
 * The syntax is just regular arithmetic, but with the option to specify a variable in brackets.
 * Inside the brackets, the variable is specified by a comma separated list of name=value.
 *
 * When using the AstEvaluator, the values for the variables are found by using a user-supplied callback function.
 * This function then uses the comma separated list (now an associative array) to find the correct value for this variable.
 *
 * The return value from the callback function should be an array!
 *
 * Grammar:
 * - Right recursive since we use recursive descent parsing
 *   Right recursive means that the productions never yield the symbol itself before anything else (directly or indirectly)
 * - S is the start symbol, upper case are terminals
 *
 * S          -> expr
 * INTFLOAT   -> (\d+(?:\.\d+)?)
 * ID         -> \w+\d*
 * expr       -> factor + expr | factor - expr | factor
 * factor     -> number * factor | number / factor | number
 * number     -> (expr) | [var] | INTFLOAT | - number | + number
 * var        -> assign | assign , var
 * assign     -> ID = INTFLOAT | ID = ID
*/

use Exception;

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

class Parser{
    /** @var $lexer Lexer */
    private $lexer;

    /**
     * @param $str string
     *
     * @throws Exception
     * @return mixed
     */
    public function parse($str){
        $this->lexer = new Lexer($str);

        $ast = $this->expression();

        if($this->lexer->get_next_token()->kind != Token::end){
            throw new Exception("KPI Parser Error: Syntax error - End expected");
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
                throw new Exception("KPI Parser Error: Syntax error - Unbalanced paranthesis");
            }

            return $node;
        }elseif($token->kind == Token::lbracket){
            $node = $this->variable();
            $expected_rbracket = $this->lexer->get_next_token();
            if($expected_rbracket->kind != Token::rbracket){
                throw new Exception("KPI Parser Error: Syntax error - Unbalanced brackets");
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
            throw new Exception("KPI Parser Error: Syntax error - Not a number: " . $token->value);
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
            throw new Exception('Expected ID, got ' . $token->value);
        }

        $name = $token->value;

        $token = $this->lexer->get_next_token();
        if($token->kind != Token::equals){
            throw new Exception('Expected equal sign, got ' . $token->value);
        }

        $token = $this->lexer->get_next_token();
        if($token->kind != Token::id and $token->kind != Token::number){
            throw new Exception('Expected ID or number, got ' . $token->value);
        }

        return array($name => $token->value);
    }
}