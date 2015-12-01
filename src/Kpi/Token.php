<?php namespace Mongotd\Kpi;

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