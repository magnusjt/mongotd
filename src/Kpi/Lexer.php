<?php namespace Mongotd\Kpi;

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