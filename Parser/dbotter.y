/*                  Copyright (c) Mindstone 2004
 *                                            
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 1, or (at your option)
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program: the file COPYING contains this copy.
 * if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
*/


%{

#include <stdio.h>
#include "dbotterparse.h"

//#define PP ((void*)(((* parse_parm)parm)->foo))
#define PP (parm)

%}


/*
** 'pure_parser' tells bison to use no global variables and create a
** reentrant parser.
*/

%pure_parser
%parse-param {parse_parm *parm}
%parse-param {void *scanner}
%lex-param {yyscan_t *scanner}


%token VARIABLE
%token FILEEND
 
%token INT
%token FLOAT
%token DATE
%token TIME
%token STRING
%token ID 
%token URI
%token CONST
%token VAR



%% /* Grammar rules and actions follow */

input:    /* empty */
          | sentencelist       { 
			       (parm->result)=$1;             
			     }
;

sentence:  assertion         { $$ = $1; }          
;

sentencelist: sentence              { $$ = MKWGPAIR(PP,$1,MKWGNIL); }
            | sentencelist sentence { $$ = MKWGPAIR(PP,$2,$1); }
;


assertion: primsentence '.'      { $$ = $1; }
;
	

primsentence: term 	         { $$ = MKWGPAIR(PP,$1,MKWGNIL); }        
            | loglist               { $$ = $1; }
;

loglist:   term                   { $$ = MKWGPAIR(PP,$1,MKWGNIL); }
         | term '|' loglist         { $$ = MKWGPAIR(PP,$1,$3); }
;

term:      prim                   { $$ = $1; }     
         | prim '(' ')'           { $$ = MKWGPAIR(PP,$1,NULL); }  
         | prim '(' termlist ')'  { $$ = MKWGPAIR(PP,$1,$3); }           
         | '-' term               { $$ = MKWGPAIR(PP,MKWGCONST(PP,"not"),MKWGPAIR(PP,$2,MKWGNIL)); }	 
;


termlist:    term                { $$ = MKWGPAIR(PP,$1,MKWGNIL); } 
	         | term ',' termlist   { $$ = MKWGPAIR(PP,$1,$3); }
;
	  

prim:     INT             { $$ = MKWGINT(PP,$1); }
        | FLOAT           { $$ = MKWGFLOAT(PP,$1); }        
	      | DATE            { $$ = MKWGDATE(PP,$1); }
	      | TIME            { $$ = MKWGTIME(PP,$1); }
        | STRING          { $$ = MKWGSTRING(PP,$1); }
	      | VAR             { $$ = MKWGVAR(PP,$1); }
        | URI	            { $$ = MKWGURI(PP,$1); }
        | ID	            { $$ = MKWGCONST(PP,$1); }
        | CONST	           { $$ = MKWGCONST(PP,$1); }
;


	
%%

