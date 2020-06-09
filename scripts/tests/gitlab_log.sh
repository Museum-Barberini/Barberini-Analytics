#!/bin/bash
# Library for printing folded gitlab sections. See also:
# https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/ci/pipelines/\
# index.md#custom-collapsible-sections

start_section() {
	echo -e "section_start:${date +%s}:{$1}\r\e[0K${2}"
}

end_section() {
    echo -e "section_end:${date +%s}:${1}\r\e[0K"
}

exit 1  # This is a library and shall not be executed
