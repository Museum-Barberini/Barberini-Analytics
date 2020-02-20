# orders
# with scraping, 600.000
# 7 days / 2-4 weeks at a time
# OrdersToDB
# OrderContainsToDB() 

import datetime as dt
import subprocess as sp

for week_offset in range(250):
    
    print(week_offset)
    
    today = dt.date.today() - dt.timedelta(weeks=week_offset)
    sp.run(
        "make luigi-clean".split()
    )
    sp.run(
        f"luigi --module gomus.orders OrdersToDB --today {today}".split()
    )
    
print("\n------------\n completed successfully \n -----------")
