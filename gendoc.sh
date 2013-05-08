#!/bin/sh
#
# run with ASCIIDOC_XSL=/path/to/stylesheets ./gendoc.sh [ destdir ]

# default stylesheets path
[ "X${ASCIIDOC_XSL}" = "X" ] && ASCIIDOC_XSL="/etc/asciidoc/docbook-xsl"

# destination directory
DESTDIR=$1
[ "X${DESTDIR}" = "X" ] && DESTDIR=Doc

iconv -f latin1 -t utf-8 Doc/Manual.txt |\
 sed -f Doc/Manual2html.sed |\
 asciidoc -b docbook - |\
 xsltproc --nonet ${ASCIIDOC_XSL}/xhtml.xsl - > ${DESTDIR}/Manual.html

iconv -f latin1 -t utf-8 Doc/python.txt |\
 sed -f Doc/python2html.sed |\
 asciidoc -b docbook - |\
 xsltproc --nonet ${ASCIIDOC_XSL}/xhtml.xsl - > ${DESTDIR}/python.html

# add Doxygen / PyDoc generation here as needed
