import acc


def compute_region_date(region, date):
    # acc.transit_accessibility(region, date, "MP")
    # acc.transit_accessibility(region_in, date_in, "PM")
    # acc.transit_accessibility(region_in, date_in, "WE")
    acc.levelofservice(region, date)
    # return None

compute_region_date("New York", "2020-05-10")
# compute_region_date("New York", "2020-09-20")
# compute_region_date("New York", "2020-10-11")


# install packages

# fares to block groups

# test for a date with just 2 times


# so the travel times for 02-23 have 26 million rows, but the fares table only has 18 million rows
