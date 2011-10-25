#!/usr/bin/env sh
nosetests -w tests $1 --with-coverage --cover-package=brukva --nologcapture --nocapture -v

