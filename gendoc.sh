#!/bin/sh
#
# run with ASCIIDOC_XSL=/path/to/stylesheets ./gendoc.sh [ destdir ]
#
# ASCIIDOC_XSL=/usr/share/asciidoc/docbook-xsl/ ./gendoc.sh

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

iconv -f latin1 -t utf-8 Doc/Tutorial.txt |\
 sed -f Doc/Tutorial2html.sed |\
 asciidoc -b docbook - |\
 xsltproc --nonet ${ASCIIDOC_XSL}/xhtml.xsl - > ${DESTDIR}/Tutorial.html

iconv -f latin1 -t utf-8 Doc/Install.txt |\
 sed -f Doc/Install2html.sed |\
 asciidoc -b docbook - |\
 xsltproc --nonet ${ASCIIDOC_XSL}/xhtml.xsl - > ${DESTDIR}/Install.html

iconv -f latin1 -t utf-8 Doc/Utilities.txt |\
 asciidoc -b docbook - |\
 xsltproc --nonet ${ASCIIDOC_XSL}/xhtml.xsl - > ${DESTDIR}/Utilities.html

# add Doxygen / PyDoc generation here as needed
