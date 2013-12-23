/*

dserve_util.c contains string and error handling utilities for dserve

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http. Results are given in the json format.

See http://whitedb.org/tools.html for a detailed manual.

Copyright (c) 2013, Tanel Tammet

This software is under MIT licence
*/

#include "dserve.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h> // is(x)digit, isint
#if _MSC_VER   
#define STDERR_FILENO 2
#else
#include <unistd.h> // for STDERR_FILENO
#endif

/* =============== local protos =================== */


/* =============== globals =================== */

extern struct dserve_global * dsdata;

/* =============== functions =================== */


/* *****  encode cgi params as query vals  ****** */


wg_int encode_incomp(void* db, char* incomp) {
  if (incomp==NULL || incomp=='\0') return WG_COND_EQUAL;
  else if (!strcmp(incomp,"equal"))  return WG_COND_EQUAL; 
  else if (!strcmp(incomp,"not_equal"))  return WG_COND_NOT_EQUAL; 
  else if (!strcmp(incomp,"lessthan"))  return WG_COND_LESSTHAN; 
  else if (!strcmp(incomp,"greater"))  return WG_COND_GREATER; 
  else if (!strcmp(incomp,"ltequal"))  return WG_COND_LTEQUAL;   
  else if (!strcmp(incomp,"gtequal"))  return WG_COND_GTEQUAL; 
  else err_clear_detach_halt(COND_ERR); 
  return WG_COND_EQUAL; // this return never happens  
}  

wg_int encode_intype(void* db, char* intype) {
  if (intype==NULL || intype=='\0') return 0;   
  else if (!strcmp(intype,"null"))  return WG_NULLTYPE; 
  else if (!strcmp(intype,"int"))  return WG_INTTYPE; 
  else if (!strcmp(intype,"record"))  return WG_RECORDTYPE;
  else if (!strcmp(intype,"double"))  return WG_DOUBLETYPE; 
  else if (!strcmp(intype,"str"))  return WG_STRTYPE; 
  else if (!strcmp(intype,"char"))  return WG_CHARTYPE;   
  else err_clear_detach_halt(INTYPE_ERR);
  return 0; // this return never happens
}

wg_int encode_invalue(void* db, char* invalue, wg_int type) {
  if (invalue==NULL) {
    err_clear_detach_halt(INVALUE_ERR);
    return 0; // this return never happens
  }  
  if (type==WG_NULLTYPE) return wg_encode_query_param_null(db,NULL);
  else if (type==WG_INTTYPE) {
    if (!isint(invalue)) err_clear_detach_halt(INVALUE_TYPE_ERR);      
    return wg_encode_query_param_int(db,atoi(invalue));
  } else if (type==WG_RECORDTYPE) {
    if (!isint(invalue)) err_clear_detach_halt(INVALUE_TYPE_ERR);      
    return (wg_int)atoi(invalue);
  } else if (type==WG_DOUBLETYPE) {
    if (!isdbl(invalue)) err_clear_detach_halt(INVALUE_TYPE_ERR);
    return wg_encode_query_param_double(db,strtod(invalue,NULL));
  } else if (type==WG_STRTYPE) {
    return wg_encode_query_param_str(db,invalue,NULL);
  } else if (type==WG_CHARTYPE) {
    return wg_encode_query_param_char(db,invalue[0]);
  } else if (type==0 && isint(invalue)) {
    return wg_encode_query_param_int(db,atoi(invalue));
  } else if (type==0 && isdbl(invalue)) {
    return wg_encode_query_param_double(db,strtod(invalue,NULL));
  } else if (type==0) {
    return wg_encode_query_param_str(db,invalue,NULL);
  } else {
    err_clear_detach_halt(INVALUE_TYPE_ERR);
    return 0; // this return never happens
  }
}  

/* ********  cgi query parsing  ****** */


/* query parser: split by & and =, urldecode param and value
   return param count or -1 for error 
*/

int parse_query(char* query, int ql, char* params[], char* values[]) {
  int count=0;
  int i,pi,vi;
  
  for(i=0;i<ql;i++) {
    pi=i;
    for(;i<ql;i++) {
      if (query[i]=='=') { query[i]=0; break; }      
    }
    i++;
    if (i>=ql) return -1;
    vi=i;    
    for(;i<ql;i++) {
      if (query[i]=='&') { query[i]=0; break; }  
    }
    if (count>=MAXPARAMS) return -1;    
    params[count]=urldecode(query+pi,query+pi);
    values[count]=urldecode(query+vi,query+vi);
    count++;
  }
  return count;
}    

/* urldecode used by query parser 
*/

char* urldecode(char *indst, char *src) {
  char a, b;
  char* endptr;
  char* dst;
  dst=indst;
  if (src==NULL || src[0]=='\0') return indst;
  endptr=src+strlen(src);
  while (*src) {
    if ((*src == '%') && (src+2<endptr) &&
        ((a = src[1]) && (b = src[2])) &&
        (isxdigit(a) && isxdigit(b))) {
      if (a >= 'a') a -= 'A'-'a';
      if (a >= 'A') a -= ('A' - 10);
      else a -= '0';
      if (b >= 'a') b -= 'A'-'a';
      if (b >= 'A') b -= ('A' - 10);
      else b -= '0';
      *dst++ = 16*a+b;
      src+=3;
    } else {
      *dst++ = *src++;
    }
  }
  *dst++ = '\0';
  return indst;
}


/* ****** guess string datatype ****** */


/* return 1 iff s contains numerals only
*/

int isint(char* s) {
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) return 0;
    s++;
  }
  return 1;
}  
  
/* return 1 iff s contains numerals plus single optional period only
*/


int isdbl(char* s) {
  int c=0;
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) {
      if (*s=='.') c++;
      else return 0;
      if (c>1) return 0;
    }
    s++;
  }
  return 1;
}   

/* *********  json printing ********* */


/** Print a record, handling records recursively

  The value is written into a character buffer.

  db: database pointer
  rec: rec to be printed

  buf: address of the whole string buffer start (not the start itself)
  bufsize: address of the actual pointer to start printing at in buffer
  bptr: address of the whole string buffer

  format: 0 csv, 1 json

  showid: print record id for record: 0 no show, 1 first (extra) elem of record

  depth: current depth in a nested record tree (increases from initial 0)
  maxdepth: limit on printing records nested via record pointers (0: no nesting)

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe

 
*/



int sprint_record(void *db, wg_int *rec, struct thread_data * tdata) {
  int i,limit;
  wg_int enc, len;
  char **bptr=&(tdata->bufptr);                                                            
#ifdef USE_CHILD_DB
  void *parent;
#endif  
  limit=MIN_STRLEN;
  str_guarantee_space(tdata, MIN_STRLEN);                     
  if (rec==NULL) {
    snprintf(*bptr, limit, JS_NULL);
    (*bptr)+=strlen(JS_NULL);
    return 1; 
  }
  if (tdata->format!=0) {
    // json
    **bptr= '['; 
    (*bptr)++;
  }  
#ifdef USE_CHILD_DB
  parent = wg_get_rec_owner(db, rec);
#endif
  if (1) {
    len = wg_get_record_len(db, rec);    
    if (tdata->showid) {
      // add record id (offset) as the first extra elem of record      
      snprintf(*bptr, limit-1, "%d",wg_encode_record(db,rec));
      *bptr=*bptr+strlen(*bptr);      
    }
    for(i=0; i<len; i++) {
      enc = wg_get_field(db, rec, i);
#ifdef USE_CHILD_DB
      if(parent != db)
        enc = wg_translate_hdroffset(db, parent, enc);
#endif
      str_guarantee_space(tdata, MIN_STRLEN);
      if (i || tdata->showid) { 
        if (tdata->format!=0) **bptr = ','; 
        else **bptr = CSV_SEPARATOR; 
        (*bptr)++; 
      }
      *bptr=sprint_value(db, enc, tdata);          
    }
  }
  if (tdata->format!=0) {
    // json
    str_guarantee_space(tdata, MIN_STRLEN);
    **bptr = ']';
    (*bptr)++;
  }  
  return 1;
}


/** Print a single encoded value (may recursively contain record(s)).

  The value is written into a character buffer.

  db: database pointer
  enc: encoded value to be printed

  relevant tdata fields:
    buf: address of the whole string buffer start (not the start itself)
    bufsize: address of the actual pointer to start printing at in buffer
    bufptr: address of the whole string buffer
    format: 0 csv, 1 json
    showid: print record id for record: 0 no show, 1 first (extra) elem of record
    depth: limit on records nested via record pointers (0: no nesting)
    maxdepth: limit on printing records nested via record pointers (0: no nesting)
    strenc==0: nothing is escaped at all
    strenc==1: non-ascii chars and % and " urlencoded
    strenc==2: json utf-8 encoding, not ascii-safe
    strenc==3: csv encoding, only " replaced for ""
  
  returns nr of bytes printed  
*/


char* sprint_value(void *db, wg_int enc, struct thread_data * tdata) {
  wg_int *ptrdata;
  int intdata,strl,strl1,strl2;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80]; // tmp area for dates
  int limit=MIN_STRLEN;
  char **bptr=&(tdata->bufptr);                   
  
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      str_guarantee_space(tdata, MIN_STRLEN);
      if (tdata->format!=0) {
        // json
        snprintf(*bptr, limit, JS_NULL);
        return *bptr+strlen(*bptr);
      }
      return *bptr;      
    case WG_RECORDTYPE:      
      str_guarantee_space(tdata, MIN_STRLEN);
      if (!tdata->format || tdata->depth>=tdata->maxdepth) {
        snprintf(*bptr, limit,"%d", (int)enc); // record offset (i.e. id)
        return *bptr+strlen(*bptr);
      } else {
        // recursive print
        ptrdata = wg_decode_record(db, enc);
        sprint_record(db,ptrdata,tdata);
        **bptr='\0';                 
        return *bptr;
      }      
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, "%d", intdata);
      return *bptr+strlen(*bptr);
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      str_guarantee_space(tdata, MIN_STRLEN); 
      snprintf(*bptr, limit, DOUBLE_FORMAT, doubledata);
      return *bptr+strlen(*bptr);
    case WG_FIXPOINTTYPE:
      doubledata = wg_decode_fixpoint(db, enc);
      str_guarantee_space(tdata, MIN_STRLEN); 
      snprintf(*bptr, limit, DOUBLE_FORMAT, doubledata);
      return *bptr+strlen(*bptr);
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      exdata = wg_decode_str_lang(db,enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);      
      else strl2=0; 
      str_guarantee_space(tdata, MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2)); 
      sprint_string(*bptr,(strl1+strl2),strdata,tdata->strenc);      
      if (exdata!=NULL) {
        snprintf(*bptr+strl1+1,limit,"@%s\"", exdata);
      }     
      return *bptr+strlen(*bptr);
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);      
      else strl2=0; 
      limit=MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2);
      str_guarantee_space(tdata, limit);
      if (exdata==NULL)
        snprintf(*bptr, limit, "\"%s\"", strdata);
      else
        snprintf(*bptr, limit, "\"%s:%s\"", exdata, strdata);
      return *bptr+strlen(*bptr);
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);      
      else strl2=0; 
      limit=MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2);      
      str_guarantee_space(tdata, limit);      
      snprintf(*bptr, limit, "\"%s:%s\"", exdata, strdata);
      return *bptr+strlen(*bptr);
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%c\"", (char) intdata);
      return *bptr+strlen(*bptr);
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%s\"",strbuf);
      return *bptr+strlen(*bptr);
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);        
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%s\"",strbuf+11);
      return *bptr+strlen(*bptr);
    case WG_VARTYPE:
      intdata = wg_decode_var(db, enc);
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, "\"?%d\"", intdata);
      return *bptr+strlen(*bptr);  
    case WG_BLOBTYPE:
      strdata = wg_decode_blob(db, enc);
      strl=wg_decode_blob_len(db, enc);
      limit=MIN_STRLEN+STRLEN_FACTOR*strlen(strdata);
      str_guarantee_space(tdata, limit);
      sprint_blob(*bptr,strl,strdata,tdata->strenc);
      return *bptr+strlen(*bptr);
    default:
      str_guarantee_space(tdata, MIN_STRLEN);
      snprintf(*bptr, limit, JS_TYPE_ERR);
      return *bptr+strlen(*bptr);
  }
}


/* Print string with several encoding/escaping options.
 
  It must be guaranteed beforehand that there is enough room in the buffer.
 
  bptr: direct pointer to location in buffer where to start writing
  limit: max nr of chars traversed (NOT limiting output len)
  strdata: pointer to printed string

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe
  strenc==3: csv encoding, only " replaced for ""

  For proper json tools see:

  json rfc http://www.ietf.org/rfc/rfc4627.txt
  ccan json tool http://git.ozlabs.org/?p=ccan;a=tree;f=ccan/json
  Jansson json tool https://jansson.readthedocs.org/en/latest/
  Parser http://linuxprograms.wordpress.com/category/json-c/

*/

int sprint_string(char* bptr, int limit, char* strdata, int strenc) {
  unsigned char c;
  char *sptr;
  char *hex_chars="0123456789abcdef";
  int i;
  sptr=strdata;
  *bptr++='"';
  if (sptr==NULL) {
    *bptr++='"';
    *bptr='\0'; 
    return 1;  
  }
  if (!strenc) {
    // nothing is escaped at all
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') { 
        *bptr++='"';
        *bptr='\0';  
        return 1; 
      } else {
        *bptr++=c;
      } 
    }  
  } else if (strenc==3) {
    // csv " replaced for "", no other escapes
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') { 
        *bptr++='"';
        *bptr='\0';  
        return 1; 
      } else if (c=='"') {
        *bptr++=c;        
        *bptr++=c;        
      } else {
        *bptr++=c;
      } 
    }  
  } else if (strenc==1) {
    // non-ascii chars and % and " urlencoded
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') { 
        *bptr++='"';
        *bptr='\0';  
        return 1; 
      } else if (c < ' ' || c=='%' || c=='"' || (int)c>126) {
        *bptr++='%';
        *bptr++=hex_chars[c >> 4];
        *bptr++=hex_chars[c & 0xf];
      } else {
        *bptr++=c;
      } 
    }    
  } else {
    // json encoding; chars before ' ' are are escaped with \u00
    sptr=strdata;
    for(i=0;i<limit;i++) {
      c=*sptr++;
      switch(c) {
      case '\0':
        *bptr++='"';
        *bptr='\0'; 
        return 1;  
      case '\b':
      case '\n':
      case '\r':
      case '\t':
      case '\f':
      case '"':
      case '\\':
      case '/':
        if(c == '\b') sprint_append(&bptr, "\\b", 2);
        else if(c == '\n') sprint_append(&bptr, "\\n", 2);
        else if(c == '\r') sprint_append(&bptr, "\\r", 2);
        else if(c == '\t') sprint_append(&bptr, "\\t", 2);
        else if(c == '\f') sprint_append(&bptr, "\\f", 2);
        else if(c == '"') sprint_append(&bptr, "\\\"", 2);
        else if(c == '\\') sprint_append(&bptr, "\\\\", 2);
        else if(c == '/') sprint_append(&bptr, "\\/", 2);
        break;      
      default:
        if(c < ' ') {
          sprint_append(&bptr, "\\u00", 4);
          *bptr++=hex_chars[c >> 4];
          *bptr++=hex_chars[c & 0xf];
        } else {
          *bptr++=c;
        } 
      }  
    }
  } 
  *bptr++='"';
  *bptr='\0'; 
  return 1;
}


int sprint_blob(char* bptr, int limit, char* strdata, int strenc) {
  unsigned char c;
  char *sptr;
  char *hex_chars="0123456789abcdef";
  int i;
  sptr=strdata;
  *bptr++='"';
  if (sptr==NULL) {
    *bptr++='"';
    *bptr='\0'; 
    return 1;  
  }  
  // non-ascii chars and % and " urlencoded
  for(i=0;i<limit;i++) {
    c=*sptr++;
    if (c=='\0') { 
      *bptr++='"';
      *bptr='\0';  
      return 1; 
    } else if (c < ' ' || c=='%' || c=='"' || (int)c>126) {
      *bptr++='%';
      *bptr++=hex_chars[c >> 4];
      *bptr++=hex_chars[c & 0xf];
    } else {
      *bptr++=c;
    } 
  }
  *bptr++='"';
  *bptr='\0'; 
  return 1;
}  

int sprint_append(char** bptr, char* str, int l) {
  int i;
  
  for(i=0;i<l;i++) *(*bptr)++=*str++;
  return 1;
}


/* *********** Functions for string buffer ******** */


/** Allocate a new string with length len, set last element to 0
*
*/

char* str_new(int len) {
  char* res;
  
  res = (char*) malloc(len*sizeof(char));
  if (res==NULL) {
    err_clear_detach_halt(MALLOC_ERR);
    return NULL; // never returns
  }  
  res[len-1]='\0';  
  return res;
}


/** Guarantee string space: realloc if necessary, change ptrs, set last byte to 0
*

  relevant tdata fields:
    buf: address of the whole string buffer start (not the start itself)
    bufsize: address of the actual pointer to start printing at in buffer
    bufptr: address of the whole string buffer   
*/

int str_guarantee_space(struct thread_data *tdata, int needed) {
  char* tmp;
  int newlen,used;
  char** stradr=&(tdata->buf); 
  int* strlenadr=&(tdata->bufsize); 
  char** ptr=&(tdata->bufptr); 

  if (needed>(*strlenadr-(int)((*ptr)-(*stradr)))) {  
    used=(int)((*ptr)-(*stradr));
    newlen=(*strlenadr)*2;
    if (newlen<needed) newlen=needed;
    if (newlen>MAX_MALLOC) {
      if (*stradr!=NULL) free(*stradr);
      err_clear_detach_halt(MALLOC_ERR);
      return 0; // never returns
    }
    //printf("needed %d oldlen %d used %d newlen %d \n",needed,*strlenadr,used,newlen);
    tmp=realloc(*stradr,newlen);
    if (tmp==NULL) {
      if (*stradr!=NULL) free(*stradr);
      err_clear_detach_halt(MALLOC_ERR);
      return 0; // never returns
    }     
    tmp[newlen-1]=0;   // set last byte to 0  
    //printf("oldstradr %d newstradr %d oldptr %d newptr %d \n",(int)*stradr,(int)tmp,(int)*ptr,(int)tmp+used);
    *stradr=tmp;
    *strlenadr=newlen;
    *ptr=tmp+used; 
    return 1;
  }  
  return 1;
}

/* ************  loading configuration  ************* */

int load_configuration(char* path, struct dserve_conf *conf) {
  FILE *fp;
  char *buf, *bp, *bp2, *bp3, *bend, *key;
  int bufsize=CONF_BUF_SIZE;
  int i,n,row;
  
  if (path==NULL) {
#ifdef CONF_FILE    
    path=CONF_FILE ;
#endif    
  }
  if (path==NULL) return 0;
  // read configuration file
  fp=fopen(path,READ);
  if (fp==NULL) {errprint(CONF_OPEN_ERR,path);  return 2;}    
  for(i=0;i<10;i++) {
    //printf("i %d bufsize %d\n",i,bufsize);
    buf=malloc(bufsize);
    if (buf==NULL) {errprint(CONF_MALLOC_ERR,NULL);  return 3;}
    n=fread(buf,1,bufsize,fp);
    if (n<=0) {errprint(CONF_READ_ERR,path); free(buf); return 3;}
    if (n>=bufsize) {
      // did not manage to read all
      free(buf);
      rewind(fp);
      bufsize=bufsize*2;
      if (bufsize>MAX_CONF_BUF_SIZE) {errprint(CONF_SIZE_ERR,path); free(buf); return 4;}
    } else {
      // reading successful
      break;
    }      
  }
  // parse configuration file
  bp=buf;
  bend=buf+n;
  key=NULL;
  for(row=0;;row++) {
    // parse row by row    
    if(bp>=bend) break;    
    if (*bp==' ' || *bp=='\t') {
      // row starts with whitespace
      // skip whitespace
      for(bp2=bp;bp2<bend && *bp2!='\n' && (*bp2==' ' || *bp2=='\t'); bp2++);
      if (*bp2=='#') { for(bp=bp2;bp<bend && *bp!='\n'; bp++); }
      else {
        // go to end of line or first whitespace or #
        for(bp3=bp2+1; bp3<bend && *bp3!='\n' && *bp3!='#' && *bp3!=' ' && *bp3!='\t';bp3++);      
        if (*bp3=='\n') { *bp3='\0'; bp=bp3+1; }       
        else { *bp3='\0'; for(bp=bp3;bp<bend && *bp!='\n'; bp++); }
        //printf("wsp line |%s|\n",bp2); 
        if (key!=NULL) {
         if(add_conf_key_val(conf,key,bp2)!=0){free(buf); exit(-1); };
        } else { errprint(CONF_VAL_ERR,bp2); free(buf); exit(-1); }
      } 
    } else if (*bp=='[') {      
      // goto end of line
      for(;bp<bend && *bp!='\n'; bp++); 
    } else if (*bp!='#') {
      // non-comment-non-whitespace-starting row
      for(bp2=bp;bp2<bend && *bp2!='\n' && *bp2!='='; bp2++);
      if (*bp2=='=') {
        *bp2='\0';
        // remove whitespace before =
        for(bp3=bp2-1; bp3>bp && (*bp3==' ' ||  *bp3=='\t'); bp3--) *bp3='\0';
        key=bp;
        //printf("def line |%s|\n",key);
        // skip whitespace      
        for(bp2++;bp2<bend && *bp2!='\n' && (*bp2==' ' || *bp2=='\t'); bp2++);
        // goto end of line or first whitespace or #
        for(bp3=bp2; bp3<bend && *bp3!='\n' && *bp3!='#' && *bp3!=' ' && *bp3!='\t';bp3++);
        if (*bp3=='\n') { *bp3='\0'; bp=bp3+1; }       
        else { *bp3='\0'; for(bp=bp3;bp<bend && *bp!='\n'; bp++); }
        //printf("val line |%s|\n",bp2);        
        if(add_conf_key_val(conf,key,bp2)!=0) {free(buf); exit(-1); }
      } else {
        // skip row
        bp=bp2;
      }
    } else {          
      // # as first char in a row: goto end of line
      for(;bp<bend && *bp!='\n'; bp++);            
    } 
    // pass carriage returns and newlines
    for(;bp<bend && (*bp=='\r' || *bp=='\n'); bp++);
    //printf("rem |%s|\n",bp);    
  }    
  return 0;
}

int add_conf_key_val(struct dserve_conf *conf, char* key, char* val) {
  if (!strcmp(key,CONF_DEFAULT_DBASE)) return add_slval(&(conf->default_dbase),val);
  else if (!strcmp(key,CONF_DBASES)) return add_slval(&(conf->dbases),val);
  else if (!strcmp(key,CONF_ADMIN_IPS)) return add_slval(&(conf->admin_ips),val);
  else if (!strcmp(key,CONF_WRITE_IPS)) return add_slval(&(conf->write_ips),val);
  else if (!strcmp(key,CONF_READ_IPS)) return add_slval(&(conf->read_ips),val);
  else if (!strcmp(key,CONF_ADMIN_TOKENS)) return add_slval(&(conf->admin_tokens),val);
  else if (!strcmp(key,CONF_WRITE_TOKENS)) return add_slval(&(conf->write_tokens),val);
  else if (!strcmp(key,CONF_READ_TOKENS)) return add_slval(&(conf->read_tokens),val);
  else {errprint(CONF_VAL_ERR,key); return -1;}       
}

int add_slval(struct sized_strlst *lst, char* val) {
  char **buf;
  int size;
  
  if (lst->used < lst->size) {    
    lst->vals[lst->used]=val;   
    lst->used++;
  } else if (lst->size==0) {
    buf=(char**)malloc(CONF_VALS_SIZE*sizeof(char*));
    if (buf==NULL) {errprint(CONF_MALLOC_ERR,NULL); return -1; }
    buf[0]=val;
    lst->vals=buf;   
    lst->size=CONF_VALS_SIZE;
    lst->used=1;
  } else {  
    size=lst->size*2;
    if (size>MAX_CONF_VALS_SIZE) {errprint(CONF_VALNR_ERR,NULL);  return -1; }
    buf=realloc(lst->vals,size*sizeof(char*));
    if (buf==NULL) {errprint(CONF_MALLOC_ERR,NULL);  return -1; }
    lst->vals=buf;
    lst->size=size;
    lst->vals[lst->used]=val;   
    lst->used++;   
  }
  return 0;
}

void print_conf(struct dserve_conf *conf) {
  print_conf_slval(&(conf->default_dbase),CONF_DEFAULT_DBASE);
  print_conf_slval(&(conf->dbases),CONF_DBASES);
  print_conf_slval(&(conf->admin_ips),CONF_ADMIN_IPS);
  print_conf_slval(&(conf->write_ips),CONF_WRITE_IPS);
  print_conf_slval(&(conf->read_ips),CONF_READ_IPS);
  print_conf_slval(&(conf->admin_tokens),CONF_ADMIN_TOKENS);
  print_conf_slval(&(conf->write_tokens),CONF_WRITE_TOKENS);
  print_conf_slval(&(conf->read_tokens),CONF_READ_TOKENS);
}

void print_conf_slval(struct sized_strlst *lst, char* key) {
  int i;
  printf("%s = # %d %d\n",key,lst->size,lst->used);
  for(i=0;i<lst->used;i++) {
    printf("  %s\n",lst->vals[i]);
  }
  
}

/* ************  help  ************* */

void print_help(void) {
  printf("dserve is a rest server tool for whitedb with a json or csv output\n");
  printf("There are three ways to run dserve:\n");
  printf("  * command line tool: dserve <command> [optional conffile] like \n");
  printf("    dserve 'op=search'\n");
  printf("  * cgi program under a web server: copy dserve to the cgi-bin folder,\n");
  printf("    optionally set #define CONF_FILE in dserve.h before compiling\n");
  printf("  * a standalone server: dserve <portnr> [optional conffile] like\n");
  printf("    dserve 8080 myconf.txt\n");
  printf("    or set #define DEFAULT_PORT <portnr> in dserve.h for startup without args\n");
  printf("See http://whitedb.org/server.html for a manual.\n");
}

/* ************  errors and exits  ************* */

void infoprint(char* fmt, char* param) {
#ifdef INFOPRINT  
  //fprintf(stderr,"dserve information: ");
  if (param!=NULL) fprintf(stderr,fmt,param);
  else fprintf(stderr,fmt,NULL);
#endif  
}

void warnprint(char* fmt, char* param) {
#ifdef WARNPRINT    
  //fprintf(stderr,"dserve warning: ");
  if (param!=NULL) fprintf(stderr,fmt,param);
  else fprintf(stderr,fmt,NULL);
#endif   
}

void errprint(char* fmt, char* param) {
#ifdef ERRPRINT    
  //fprintf(stderr,"dserve error: ");
  if (param!=NULL) fprintf(stderr,fmt,param);
  else fprintf(stderr,fmt,NULL);
#endif   
}

/* called in case of internal errors by the signal catcher:
   it is crucial that the locks are released and db detached */

void termination_handler(int signal) {
  int n;
  //printf("termination_handler called\n");
  clear_detach(signal);
  n=write(STDERR_FILENO, TERMINATE_ERR, strlen(TERMINATE_ERR));    
  if (n); // to suppress senseless gcc warning
  exit(-1);
}

void clear_detach(int signal) { 
  int i;  
  //printf("clear_detach called %d\n",dsdata->maxthreads);
  if (dsdata==NULL) {
    i=write(STDERR_FILENO, TERMINATE_NOGLOB_ERR, strlen(TERMINATE_NOGLOB_ERR));
  }
#ifdef SERVEROPTION  
  // avoid new further threads run and locks taken
  dsdata->threads_data[0].common->shutdown=1;
#endif  
  // clear locks
  for(i=0;(i < dsdata->maxthreads) && (i<1000); i++) {
    printf("clearing thread %d locks \n",i);
    if (dsdata->threads_data[i].db!=NULL && dsdata->threads_data[i].lock_id) {
      //printf("clear_detach freeing %d\n",i);
      if (dsdata->threads_data[i].lock_type==READ_LOCK_TYPE) {
        printf("clear_detach end_read %d\n",i);
        wg_end_read(dsdata->threads_data[i].db,dsdata->threads_data[i].lock_id);
        dsdata->threads_data[i].lock_id=0;
      } else if (dsdata->threads_data[i].lock_type==WRITE_LOCK_TYPE) {
        printf("clear_detach end_write %d\n",i);
        wg_end_write(dsdata->threads_data[i].db,dsdata->threads_data[i].lock_id);
        dsdata->threads_data[i].lock_id=0;
      }     
    }
  }
  // detach databases
  /*
  for(i=0;(i < dsdata->maxthreads) && (i<1000); i++) {
    printf("detaching thread %d database\n",i);
    if (dsdata->threads_data[i].db!=NULL) {
      wg_detach_database(dsdata->threads_data[i].db);
      dsdata->threads_data[i].db=NULL;
    }  
  }
  return;  
  */
}  

/* called in case of timeout by the signal catcher:
   it is crucial that the locks are released and db detached */

void timeout_handler(int signal) {
  //printf("timeout_handler called\n");
  err_clear_detach_halt(TIMEOUT_ERR);
}

/* normal termination call: free locks, detach, call errprint and halt */

void err_clear_detach_halt(char* errstr) {      
  //printf("err_clear_detach_halt called\n");
  clear_detach(0);  
  errhalt(errstr,0);
}  

/* normal user input / nonterminating error processing
*/

char* errhalt(char* str, struct thread_data * tdata) {
  char buf[HTTP_ERR_BUFSIZE];
  if (tdata->isserver) {
    return make_http_errstr(str);
  } else {
    snprintf(buf,HTTP_ERR_BUFSIZE,NORMAL_ERR_FORMAT,str);
    print_final(buf,tdata);
    exit(0);
  }  
}

char* make_http_errstr(char* str) {
  char *errstr;
  
  errstr=malloc(HTTP_ERR_BUFSIZE);
  if (!errstr) return NULL;
  snprintf(errstr,HTTP_ERR_BUFSIZE,NORMAL_ERR_FORMAT,str);  
  return errstr;
}
