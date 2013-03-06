/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
*
* Contact: tanel.tammet@gmail.com                 
*
* This file is part of wgandalf
*
* Wgandalf is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* Wgandalf is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file dbprolog.l
 *  Lexer rules for prolog parser
 *
 */

%{
#include <math.h>
#include <stdio.h>
#include "dbprologparse.h"
#include "dbprolog.tab.h"

/*  reentrant stuff starts */

#define PARM    yyget_extra(yyscanner)

/*

#define YY_INPUT(buffer, res, max_size)             \
    do {                                                \
        if (PARM->pos >= PARM->length)                  \
            res = YY_NULL;                              \
        else                                            \
        {                                               \
            res = PARM->length - PARM->pos;             \
            res > (int)max_size ? res = max_size : 0;   \
            memcpy(buffer, PARM->buf + PARM->pos, res); \
            PARM->pos += res;                           \
        }                                               \
    } while (0)

*/

char linebuf[1024];
char elmparsestrbuf[1024]; 
char *s; 

%}


%option reentrant 
%option bison-bridge
%option noyywrap
%option yylineno 
%option nounput
%option noinput

%x SQLSTATE
%x STRSTATE 

ATOM	[a-z#][_a-zA-Z0-9]*
VAR	[A-Z][_a-zA-Z0-9]*

%%


"%".*\n	{
	parseprintf("A lineful of comment.\n"); /* eat line comments */
	}

:-	{
	parseprintf("A :- , \"IS\"\n");
	return IS;
	}


not	{
	parseprintf("A \"not\".\n");
	return NOT;
	}

{ATOM}	{
	parseprintf("An atom: %s\n", yytext);
	*yylval=strdup(yytext);
	return ATOM;
	}

{VAR}	{
	parseprintf("A variable: %s\n", yytext);
	*yylval=strdup(yytext);
	return VAR;
	}

[0-9]+	{ parseprintf( "An integer: %s (%d)\n", yytext, atoi(yytext));
	*yylval=strdup(yytext);
	return INT;
	}

[0-9]+"."[0-9]+	{
		parseprintf( "A float: %s (%lf)\n", yytext, atof(yytext));
		*yylval=strdup(yytext);
		return FLOAT;
		}


"'"[^']+"'"	{
		parseprintf("A \'-quoted string, basically an atom: %s\n", yytext);
		*yylval = strdup(yytext) + 1; // erase the first character
		((char*)*yylval)[strlen(*yylval) - 1] = '\0'; // erase the last character '
		return ATOM;
		}

"\""[^"]+"\""	{
		parseprintf("A quoted string, basically an atom: %s\n", yytext);
	   *yylval = strdup(yytext); // erase the first character
		//((char*)*yylval)[strlen(*yylval) - 1] = '\0'; // erase the last character '
    
		return ATOM;
		}


[)] return ')';
[(] return '(';
[,] return ',';
[.] return '.';
[;] return ';';
[!] return '!';
[~] return '~';

<<EOF>>		{
		parseprintf("file end. Read %d lines.\n", yylineno);
		yyterminate();
		return 0;
		}


[\r\n]     { yylineno++; }  
[ \t]+     ;/* eat up whitespace */
.         { parseprintf( "Unrecognized character: %s\n", yytext ); }


%%


void wg_yyprologerror(parse_parm* parm, void* scanner, char* msg) {
  //printf("\n yyerror called with xx msg %s\n",msg);
  printf("parse error at prolog file %s line %d text fragment %s: %s \n", 
          parm->filename,yyget_lineno(scanner),yyget_text(scanner),msg);  
}

   
