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

 /** @file dbotter.l
 *  Lexer rules for otter parser
 *
 */


%{
#include <math.h>
#include <stdio.h>
#include "dbotterparse.h"
#include "dbotter.tab.h"

/*  reentrant stuff starts */

#define PARM    yyget_extra(yyscanner)

/*

YYERROR_VERBOSE. 

*/



/*
#define YY_INPUT(buffer, res, max_size)             \
  if (PARM->foo!=NULL) { \
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
    } while (0);\
  } else { \
    int c = getchar(); \
    res = ((c == EOF) ? YY_NULL : (buffer[0] = c, 1)); \
  } 
*/



#define YY_INPUT(buffer, res, max_size)             \
  if (PARM->foo!=NULL) { \
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
    } while (0);\
  } else { \
    int n = fread(buffer,1,max_size,stdin); \
    if (n<=0) res=YY_NULL;\
    else res=n; \
  } 



/*

void
lex_parsestr(const char *s)
{
        YY_BUFFER_STATE yyhandle;

        yyhandle = YY_CURRENT_BUFFER;
        yy_scan_string(s);
        yylex();
        yy_delete_buffer(YY_CURRENT_BUFFER);
        yy_switch_to_buffer(yyhandle);
}

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

%x STRSTATE 
%x QUOTESTATE
%x COMMENT

DIGIT    [0-9]
ID       [A-z][A-z0-9_:+\-*/<>=]*

%%

"+"|"!-"|"*"|"/"|"<"|">"|"="|"<="|">="  {
              parseprintf( "an op: %s", yytext);
              *yylval=yytext;	      
              return URI; 
	    }


		  
{DIGIT}+    { parseprintf( "An integer: %s (%d)\n",yytext,atoi(yytext));	           
              *yylval=yytext;              
              return INT; 
	    }
{DIGIT}+"."{DIGIT}+    {
              parseprintf( "A float: %s", yytext);
              *yylval=yytext;	      
              return FLOAT; 
	    }
	    
{DIGIT}{DIGIT}{DIGIT}{DIGIT}"-"{DIGIT}{DIGIT}"-"{DIGIT}{DIGIT} {
              parseprintf( "A date: %s\n", yytext);
              *yylval=yytext;	      
              return DATE;      
      }
      
{DIGIT}{DIGIT}":"{DIGIT}{DIGIT}":"{DIGIT}{DIGIT} {
              parseprintf( "A time: %s\n", yytext);			  
	            *yylval=yytext; 
              return TIME; 
	    }  	    

\"              { BEGIN STRSTATE; s = elmparsestrbuf; } 
<STRSTATE>\\n   { *s++ = '\n'; } 
<STRSTATE>\\t   { *s++ = '\t'; } 
<STRSTATE>\\\"  { *s++ = '\"'; } 
<STRSTATE>\"    { *s = 0; BEGIN 0; 
                  parseprintf("found '%s'\n", elmparsestrbuf); 
		              *yylval=elmparsestrbuf;
		              return STRING;
                } 
<STRSTATE>\n    { *s++ = '\n'; 
                 /* parseprintf("elm parser error: invalid string (newline in string)"); 
                    exit(1); 
		             */   
	        } 
<STRSTATE>.     { *s++ = *yytext; }


\'              { BEGIN QUOTESTATE; s = elmparsestrbuf; } 
<QUOTESTATE>\\n   { *s++ = '\n'; } 
<QUOTESTATE>\\t   { *s++ = '\t'; } 
<QUOTESTATE>\\\'  { *s++ = '\''; } 
<QUOTESTATE>\'    { *s = 0; BEGIN 0; 
                  parseprintf("found '%s'\n", elmparsestrbuf); 
		              *yylval=elmparsestrbuf;
		              return URI;
                } 
<QUOTESTATE>\n    { *s++ = '\n'; 
                 /* parseprintf("elm parser error: invalid string (newline in quote)"); 
                    exit(1); 
		             */   
	        } 
<QUOTESTATE>.     { *s++ = *yytext; }


"/*"         BEGIN(COMMENT);  

<COMMENT>[^*\n]*        /* eat anything that's not a '*' */
<COMMENT>"*"+[^*/\n]*   /* eat up '*'s not followed by '/'s */
<COMMENT>\n             ;
<COMMENT>"*"+"/"        BEGIN(INITIAL);

{ID}  {
	      parseprintf( "An identifier: %s\n", yytext );
	      *yylval=yytext;
	      return URI;
	    }  

[?]{ID} {
	        parseprintf( "A variable: %s\n", yytext );
	        *yylval=yytext;
	        return VAR;
	      } 

"%".*\n { } /* eat up line comment until end of line */


[-] return '-';
[|] return '|';
[)] return ')';
[(] return '(';
[}] return '}';
[{] return '{';
[,] return ',';
[.] return '.';

<<EOF>>      { parseprintf("file end\n"); 
               //return FILEEND; 
	       yyterminate();
	     }
[\r\n] {} 
[ \t]+     /* eat up whitespace */




.         parseprintf( "Unrecognized character: %s\n", yytext );

%%


void wg_yyottererror (parse_parm* parm, void* scanner, char* msg) {
  //printf("\n yyerror called with xx msg %s\n",msg);
  printf("%s at otter file %s line %d text fragment:\n%s\n", 
          msg,parm->filename,yyget_lineno(scanner),yyget_text(scanner));  
}

   
