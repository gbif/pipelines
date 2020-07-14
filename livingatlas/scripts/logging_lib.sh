#!/bin/bash

#
# Simple logging mechanism for Bash
#
# Author: Michael Wayne Goodman <goodman.m.w@gmail.com>
# Thanks: Jul for the idea to add a datestring. See:
# http://www.goodmami.org/2011/07/simple-logging-in-bash-scripts/#comment-5854
# Thanks: @gffhcks for noting that inf() and debug() should be swapped,
#         and that critical() used $2 instead of $1
#
# LA modified version based on:
#   http://www.ludovicocaldara.net/dba/bash-tips-4-use-logging-levels/
#
# License: Public domain; do as you wish
#

verbosity=$1
no_colors=$2
dr=$3

if [[ $no_colors = false ]]
then
  colblk='\033[0;30m' # Black - Regular
  colred='\033[0;31m' # Red
  colgrn='\033[0;32m' # Green
  colylw='\033[0;33m' # Yellow
  colpur='\033[0;35m' # Purpl
  colgre='\033[0;90m' # Grey
  colrst='\033[0m'    # Text Reset
fi

### verbosity levels
silent_lvl=0
crt_lvl=1
err_lvl=2
wrn_lvl=3
ntf_lvl=4
inf_lvl=5
dbg_lvl=6

## esilent prints output even in silent mode
function log.silent ()  { verb_lvl=$silent_lvl elog "$@" ;}
function log.notify ()  { verb_lvl=$ntf_lvl elog "$@" ;}
function log.ok ()      { verb_lvl=$ntf_lvl elog "SUCCESS - $@" ;}
function log.warn ()    { verb_lvl=$wrn_lvl elog "[$dr] ${colylw}WARN${colrst}  $@" ;}
function log.info ()    { verb_lvl=$inf_lvl elog "[$dr] ${colgrn}INFO${colrst}  $@" ;}
function log.debug ()   { verb_lvl=$dbg_lvl elog "[$dr] ${colgre}DEBUG${colrst} $@" ;}
function log.error ()   { verb_lvl=$err_lvl elog "[$dr] ${colred}ERROR${colrst} $@" ;}
function log.crit ()    { verb_lvl=$crt_lvl elog "[$dr] ${colpur}FATAL${colrst} $@" ;}

function elog() {
    if [ $verbosity -ge $verb_lvl ]; then
        datestring=`date +"%y/%m/%d ${colgre}%H:%M:%S${colrst}"`
        echo -e "$datestring $@"
    fi
}
