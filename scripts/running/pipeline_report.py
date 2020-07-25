import datetime as dt
import glob
from pathlib import Path
import re


today = dt.date.today()

log_dates = [today - dt.timedelta(days=days) for days in range(7, 0, -1)]
log_names = [
	name
	for date in log_dates
	for name in glob.glob(f'/var/log/barberini-analytics/*-{date}.log')
]
log_string = '\n'.join(
	Path(name).read_text()
	for name in log_names
)
print('\n\n'.join(
	match.group(0)
	for match in re.finditer(
		r'^(ERROR|WARNING): (?!\[pid .*\]).+(\n\1: (?!\[pid ^C\]).*)*',
		log_string,
		flags=re.MULTILINE
	)
))

# LATEST TODO: What to do with these results?
# Group by tasks via more fancy regex?

# To send an email, we need the docker.
