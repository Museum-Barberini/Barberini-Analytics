#!/bin/bash


{
python3 <<-EOF
import datetime as dt
today = dt.date.today()
print('\n'.join(str(today - dt.timedelta(days=days)) for days in range(7, 0, -1)))
EOF
} | xargs -l -I{} find /var/log/barberini-analytics -name '*-{}.log' \
  | xargs -l grep -Poz '\n(ERROR|WARNING): (?!\[pid .*\])[\s\S]*' \
  | sort | uniq -c | sort -nr

if $?
then
	docker-compose -p $USER -f $BASEDIR/docker/docker-compose.yml exec -T \
		barberini_analytics_luigi python3 <<-EOF

	
	EOF
    docker-compose -p $USER -f $BASEDIR/docker/docker-compose.yml exec -T \
            barberini_analytics_luigi /app/scripts/running/notify_external_error.py $1
fi
