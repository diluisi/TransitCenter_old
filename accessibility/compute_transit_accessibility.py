import acc


def compute_region_date(region, date):
    # acc.transit_accessibility(region, date, "MP")
    # acc.transit_accessibility(region_in, date_in, "PM")
    # acc.transit_accessibility(region_in, date_in, "WE")
    acc.levelofservice(region, date)
    # return None

compute_region_date("Boston", "2020-02-29")


# install packages

# fares to block groups

# test for a date with just 2 times
