/*
 * Copyright (c) 2007-2011, Lloyd Hilaiel <lloyd@hilaiel.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef YAJL_API_H
#define YAJL_API_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAJL_MAX_DEPTH 128

#define YAJL_API

/** pointer to a malloc function, supporting client overriding memory
 *  allocation routines */
typedef void * (*yajl_malloc_func)(void *ctx, size_t sz);

/** pointer to a free function, supporting client overriding memory
 *  allocation routines */
typedef void (*yajl_free_func)(void *ctx, void * ptr);

/** pointer to a realloc function which can resize an allocation. */
typedef void * (*yajl_realloc_func)(void *ctx, void * ptr, size_t sz);

/** A structure which can be passed to yajl_*_alloc routines to allow the
 *  client to specify memory allocation functions to be used. */
typedef struct
{
    /** pointer to a function that can allocate uninitialized memory */
    yajl_malloc_func malloc;
    /** pointer to a function that can resize memory allocations */
    yajl_realloc_func realloc;
    /** pointer to a function that can free memory allocated using
     *  reallocFunction or mallocFunction */
    yajl_free_func free;
    /** a context pointer that will be passed to above allocation routines */
    void * ctx;
} yajl_alloc_funcs;

/**
 * \file yajl_parse.h
 * Interface to YAJL's JSON stream parsing facilities.
 */

/** error codes returned from this interface */
typedef enum {
    /** no error was encountered */
    yajl_status_ok,
    /** a client callback returned zero, stopping the parse */
    yajl_status_client_canceled,
    /** An error occured during the parse.  Call yajl_get_error for
     *  more information about the encountered error */
    yajl_status_error
} yajl_status;

/** attain a human readable, english, string for an error */
YAJL_API const char * yajl_status_to_string(yajl_status code);

/** an opaque handle to a parser */
typedef struct yajl_handle_t * yajl_handle;

/** yajl is an event driven parser.  this means as json elements are
 *  parsed, you are called back to do something with the data.  The
 *  functions in this table indicate the various events for which
 *  you will be called back.  Each callback accepts a "context"
 *  pointer, this is a void * that is passed into the yajl_parse
 *  function which the client code may use to pass around context.
 *
 *  All callbacks return an integer.  If non-zero, the parse will
 *  continue.  If zero, the parse will be canceled and
 *  yajl_status_client_canceled will be returned from the parse.
 *
 *  \attention {
 *    A note about the handling of numbers:
 *
 *    yajl will only convert numbers that can be represented in a
 *    double or a 64 bit (long long) int.  All other numbers will
 *    be passed to the client in string form using the yajl_number
 *    callback.  Furthermore, if yajl_number is not NULL, it will
 *    always be used to return numbers, that is yajl_integer and
 *    yajl_double will be ignored.  If yajl_number is NULL but one
 *    of yajl_integer or yajl_double are defined, parsing of a
 *    number larger than is representable in a double or 64 bit
 *    integer will result in a parse error.
 *  }
 */
typedef struct {
    int (* yajl_null)(void * ctx);
    int (* yajl_boolean)(void * ctx, int boolVal);
    int (* yajl_integer)(void * ctx, long long integerVal);
    int (* yajl_double)(void * ctx, double doubleVal);
    /** A callback which passes the string representation of the number
     *  back to the client.  Will be used for all numbers when present */
    int (* yajl_number)(void * ctx, const char * numberVal,
                        size_t numberLen);

    /** strings are returned as pointers into the JSON text when,
     * possible, as a result, they are _not_ null padded */
    int (* yajl_string)(void * ctx, const unsigned char * stringVal,
                        size_t stringLen);

    int (* yajl_start_map)(void * ctx);
    int (* yajl_map_key)(void * ctx, const unsigned char * key,
                         size_t stringLen);
    int (* yajl_end_map)(void * ctx);

    int (* yajl_start_array)(void * ctx);
    int (* yajl_end_array)(void * ctx);
} yajl_callbacks;

/** allocate a parser handle
 *  \param callbacks  a yajl callbacks structure specifying the
 *                    functions to call when different JSON entities
 *                    are encountered in the input text.  May be NULL,
 *                    which is only useful for validation.
 *  \param afs        memory allocation functions, may be NULL for to use
 *                    C runtime library routines (malloc and friends)
 *  \param ctx        a context pointer that will be passed to callbacks.
 */
YAJL_API yajl_handle yajl_alloc(const yajl_callbacks * callbacks,
                                yajl_alloc_funcs * afs,
                                void * ctx);


/** configuration parameters for the parser, these may be passed to
 *  yajl_config() along with option specific argument(s).  In general,
 *  all configuration parameters default to *off*. */
typedef enum {
    /** Ignore javascript style comments present in
     *  JSON input.  Non-standard, but rather fun
     *  arguments: toggled off with integer zero, on otherwise.
     *
     *  example:
     *    yajl_config(h, yajl_allow_comments, 1); // turn comment support on
     */
    yajl_allow_comments = 0x01,
    /**
     * When set the parser will verify that all strings in JSON input are
     * valid UTF8 and will emit a parse error if this is not so.  When set,
     * this option makes parsing slightly more expensive (~7% depending
     * on processor and compiler in use)
     *
     * example:
     *   yajl_config(h, yajl_dont_validate_strings, 1); // disable utf8 checking
     */
    yajl_dont_validate_strings     = 0x02,
    /**
     * By default, upon calls to yajl_complete_parse(), yajl will
     * ensure the entire input text was consumed and will raise an error
     * otherwise.  Enabling this flag will cause yajl to disable this
     * check.  This can be useful when parsing json out of a that contains more
     * than a single JSON document.
     */
    yajl_allow_trailing_garbage = 0x04,
    /**
     * Allow multiple values to be parsed by a single handle.  The
     * entire text must be valid JSON, and values can be seperated
     * by any kind of whitespace.  This flag will change the
     * behavior of the parser, and cause it continue parsing after
     * a value is parsed, rather than transitioning into a
     * complete state.  This option can be useful when parsing multiple
     * values from an input stream.
     */
    yajl_allow_multiple_values = 0x08,
    /**
     * When yajl_complete_parse() is called the parser will
     * check that the top level value was completely consumed.  I.E.,
     * if called whilst in the middle of parsing a value
     * yajl will enter an error state (premature EOF).  Setting this
     * flag suppresses that check and the corresponding error.
     */
    yajl_allow_partial_values = 0x10
} yajl_option;

/** allow the modification of parser options subsequent to handle
 *  allocation (via yajl_alloc)
 *  \returns zero in case of errors, non-zero otherwise
 */
YAJL_API int yajl_config(yajl_handle h, yajl_option opt, ...);

/** free a parser handle */
YAJL_API void yajl_free(yajl_handle handle);

/** Parse some json!
 *  \param hand - a handle to the json parser allocated with yajl_alloc
 *  \param jsonText - a pointer to the UTF8 json text to be parsed
 *  \param jsonTextLength - the length, in bytes, of input text
 */
YAJL_API yajl_status yajl_parse(yajl_handle hand,
                                const unsigned char * jsonText,
                                size_t jsonTextLength);

/** Parse any remaining buffered json.
 *  Since yajl is a stream-based parser, without an explicit end of
 *  input, yajl sometimes can't decide if content at the end of the
 *  stream is valid or not.  For example, if "1" has been fed in,
 *  yajl can't know whether another digit is next or some character
 *  that would terminate the integer token.
 *
 *  \param hand - a handle to the json parser allocated with yajl_alloc
 */
YAJL_API yajl_status yajl_complete_parse(yajl_handle hand);

/** get an error string describing the state of the
 *  parse.
 *
 *  If verbose is non-zero, the message will include the JSON
 *  text where the error occured, along with an arrow pointing to
 *  the specific char.
 *
 *  \returns A dynamically allocated string will be returned which should
 *  be freed with yajl_free_error
 */
YAJL_API unsigned char * yajl_get_error(yajl_handle hand, int verbose,
                                        const unsigned char * jsonText,
                                        size_t jsonTextLength);

/**
 * get the amount of data consumed from the last chunk passed to YAJL.
 *
 * In the case of a successful parse this can help you understand if
 * the entire buffer was consumed (which will allow you to handle
 * "junk at end of input").
 *
 * In the event an error is encountered during parsing, this function
 * affords the client a way to get the offset into the most recent
 * chunk where the error occured.  0 will be returned if no error
 * was encountered.
 */
YAJL_API size_t yajl_get_bytes_consumed(yajl_handle hand);

/** free an error returned from yajl_get_error */
YAJL_API void yajl_free_error(yajl_handle hand, unsigned char * str);

/**
 * \file yajl_gen.h
 * Interface to YAJL's JSON generation facilities.
 */

/** generator status codes */
typedef enum {
    /** no error */
    yajl_gen_status_ok = 0,
    /** at a point where a map key is generated, a function other than
     *  yajl_gen_string was called */
    yajl_gen_keys_must_be_strings,
    /** YAJL's maximum generation depth was exceeded.  see
     *  YAJL_MAX_DEPTH */
    yajl_max_depth_exceeded,
    /** A generator function (yajl_gen_XXX) was called while in an error
     *  state */
    yajl_gen_in_error_state,
    /** A complete JSON document has been generated */
    yajl_gen_generation_complete,
    /** yajl_gen_double was passed an invalid floating point value
     *  (infinity or NaN). */
    yajl_gen_invalid_number,
    /** A print callback was passed in, so there is no internal
     * buffer to get from */
    yajl_gen_no_buf,
    /** returned from yajl_gen_string() when the yajl_gen_validate_utf8
     *  option is enabled and an invalid was passed by client code.
     */
    yajl_gen_invalid_string
} yajl_gen_status;

/** an opaque handle to a generator */
typedef struct yajl_gen_t * yajl_gen;

/** a callback used for "printing" the results. */
typedef void (*yajl_print_t)(void * ctx,
                             const char * str,
                             size_t len);

/** configuration parameters for the parser, these may be passed to
 *  yajl_gen_config() along with option specific argument(s).  In general,
 *  all configuration parameters default to *off*. */
typedef enum {
    /** generate indented (beautiful) output */
    yajl_gen_beautify = 0x01,
    /**
     * Set an indent string which is used when yajl_gen_beautify
     * is enabled.  Maybe something like \\t or some number of
     * spaces.  The default is four spaces ' '.
     */
    yajl_gen_indent_string = 0x02,
    /**
     * Set a function and context argument that should be used to
     * output generated json.  the function should conform to the
     * yajl_print_t prototype while the context argument is a
     * void * of your choosing.
     *
     * example:
     *   yajl_gen_config(g, yajl_gen_print_callback, myFunc, myVoidPtr);
     */
    yajl_gen_print_callback = 0x04,
    /**
     * Normally the generator does not validate that strings you
     * pass to it via yajl_gen_string() are valid UTF8.  Enabling
     * this option will cause it to do so.
     */
    yajl_gen_validate_utf8 = 0x08,
    /**
     * the forward solidus (slash or '/' in human) is not required to be
     * escaped in json text.  By default, YAJL will not escape it in the
     * iterest of saving bytes.  Setting this flag will cause YAJL to
     * always escape '/' in generated JSON strings.
     */
    yajl_gen_escape_solidus = 0x10
} yajl_gen_option;

/** allow the modification of generator options subsequent to handle
 *  allocation (via yajl_alloc)
 *  \returns zero in case of errors, non-zero otherwise
 */
YAJL_API int yajl_gen_config(yajl_gen g, yajl_gen_option opt, ...);

/** allocate a generator handle
 *  \param allocFuncs an optional pointer to a structure which allows
 *                    the client to overide the memory allocation
 *                    used by yajl.  May be NULL, in which case
 *                    malloc/free/realloc will be used.
 *
 *  \returns an allocated handle on success, NULL on failure (bad params)
 */
YAJL_API yajl_gen yajl_gen_alloc(const yajl_alloc_funcs * allocFuncs);

/** free a generator handle */
YAJL_API void yajl_gen_free(yajl_gen handle);

YAJL_API yajl_gen_status yajl_gen_integer(yajl_gen hand, long long int number);
/** generate a floating point number.  number may not be infinity or
 *  NaN, as these have no representation in JSON.  In these cases the
 *  generator will return 'yajl_gen_invalid_number' */
YAJL_API yajl_gen_status yajl_gen_double(yajl_gen hand, double number);
YAJL_API yajl_gen_status yajl_gen_number(yajl_gen hand,
                                         const char * num,
                                         size_t len);
YAJL_API yajl_gen_status yajl_gen_string(yajl_gen hand,
                                         const unsigned char * str,
                                         size_t len);
YAJL_API yajl_gen_status yajl_gen_null(yajl_gen hand);
YAJL_API yajl_gen_status yajl_gen_bool(yajl_gen hand, int boolean);
YAJL_API yajl_gen_status yajl_gen_map_open(yajl_gen hand);
YAJL_API yajl_gen_status yajl_gen_map_close(yajl_gen hand);
YAJL_API yajl_gen_status yajl_gen_array_open(yajl_gen hand);
YAJL_API yajl_gen_status yajl_gen_array_close(yajl_gen hand);

/** access the null terminated generator buffer.  If incrementally
 *  outputing JSON, one should call yajl_gen_clear to clear the
 *  buffer.  This allows stream generation. */
YAJL_API yajl_gen_status yajl_gen_get_buf(yajl_gen hand,
                                          const unsigned char ** buf,
                                          size_t * len);

/** clear yajl's output buffer, but maintain all internal generation
 *  state.  This function will not "reset" the generator state, and is
 *  intended to enable incremental JSON outputing. */
YAJL_API void yajl_gen_clear(yajl_gen hand);

#ifdef __cplusplus
}
#endif

#endif /* YAJL_API_H */
