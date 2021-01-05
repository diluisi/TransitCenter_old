import acc


def compute_region_date(region, date):
    # acc.transit_accessibility(region, date, "MP")
    # acc.transit_accessibility(region_in, date_in, "PM")
    # acc.transit_accessibility(region_in, date_in, "WE")
    acc.levelofservice(region, date)
    # return None

# compute_region_date("New York", "2020-02-23")
# compute_region_date("New York", "2020-05-10")
# compute_region_date("New York", "2020-09-20")
compute_region_date("New York", "2020-10-11")
