#!/bin/bash
# Library for printing folded gitlab sections. See also:
# https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/ci/pipelines/\
# index.md#custom-collapsible-sections

# Usage: start_section id "Some header"
start_section() {
	if [ -z "$CI" ]
	then
		echo $2
	else
		echo -e "section_start:$(date +%s):${1}\r\e[0K${2}"
	fi
}

# Usage: end_section id
end_section() {
	if [ -z "$CI" ]
	then
		echo "Done."
	else
    	echo -e "section_end:$(date +%s):${1}\r\e[0K"
	fi
}
