#!/bin/bash

erl -sname foo -setcookie example -detached

./fmke_populator/_build/default/bin/fmke_populator $@
