# bash completion for la-pipelines         -*- shell-script -*-
#
# For your local dev environment in bash run:
#   source la-pipelines-bash-completion.sh
# or move to:
#   /etc/bash_completion.d/la-pipelines.sh
#
# This is installed by the debian package

_la-pipelines()
{
    local cur prev words cword curcmd
    _init_completion -n = || return

    help_options="--help -h --version"
    def_options="$help_options --config= --extra-args= --dry-run --no-colors --debug"
    options="$def_options dwca-avro interpret uuid export-latlng sample sample-avro index do-all"
    COMPREPLY=( $( compgen -W "$options" -- "$cur" ) )

    if [ $prev = "-h" ] || [ $prev = "--help" ] || [ $prev = "--v" ] || [ $prev = "--version" ] ; then
        suggestions=""
        COMPREPLY=( $( compgen -W "$suggestions" -- "$cur" ) )
    elif [ $prev = "dwca-avro" ] || [ $prev = "interpret" ] || [ $prev = "uuid" ] || [ $prev = "export-latlng" ] || [ $prev = "sample" ] || [ $prev = "sample-avro" ] || [ $prev = "sample-avro" ]; then
        curcmd=$prev
        suggestions="all dr"
        COMPREPLY=( $( compgen -W "$suggestions" -- "$cur" ) )
    elif [ $prev = "all" ] || [[ $prev = dr?* ]] ; then
        suggestions="--local --embedded --cluster"
        COMPREPLY=( $( compgen -W "$suggestions" -- "$cur" ) )
    elif [ $prev = "dr" ] ; then
        # Work in progress drTAB autocopletion
        # move back
        echo -en "\033[1D"
        # delete til the end
        echo -en "\033[K"
        suggestions="$( ( ls -1 /data/biocache-load/ | sed 's/dr//g' ) )  "
        COMPREPLY=( $( compgen -W "$suggestions" -- "$cur" ) )
    fi
}

complete -F _la-pipelines la-pipelines
