#!/bin/bash
#
# MySQL Database Prepatation script.
# Author:  Michael Kepple
# Date:    15 Feb 2014
# Note:    Ensure that mysql user is owner of all database folders and that 
#          script invoker has necessary privileges to mv.
# Note2:   Functions will operate on databases containing '200*' substring.
#          Be careful.
#
read -p "MySQL Username: " uname
stty -echo
read -p "Password: " passw; echo
stty echo

# Remove '-mysql' suffix from all databases, upgrade from MySQL 5.0 format
initialize()
{
    for Dir in `find . -type d`
    do
        if [[ $Dir =~ .*_200.*_ ]] && ls $Dir/*.frm &> /dev/null; then
            echo "Database found -> "${Dir##*/}""
            oldName="\`#mysql50#${Dir:2:${#Dir}}\`"
            if [ "${Dir: -6}" = "-mysql" ]; then
                mv $Dir ${Dir:0:${#Dir}-6}
            fi 
            mysql --user="$uname" --password="$passw" --execute "ALTER DATABASE "$oldName" UPGRADE DATA DIRECTORY NAME;" &> /dev/null
        fi
    done
}

# Remve commas and quotation marks from specified table in database
sanitize_table()
{
    args=(${1//./ })
    table=${args[0]}
    field=${args[1]}
    echo "Sanitizing Field:  $field"
    echo "In table:          $table"
    for Dir in `find . -type d`
    do
        if [[ $Dir =~ .*_200.*_ ]] && ls $Dir/$table.frm &> /dev/null; then
            db="${Dir##*/}"
            echo "Database found: $db"
            mysql --user="$uname" --password="$passw" --execute "UPDATE "$db"."$table" SET "$field"=replace(replace("$field",',',''),'\"','');"
        fi
    done
}

while getopts ":is:" opt; do
  case $opt in
   i) initialize
      ;;
   s) sanitize_table $OPTARG
  esac
done
