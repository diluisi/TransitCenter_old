import acc


def compute_accessibility(region, date):
    acc.transit_accessibility(region, date, "MP")
    # acc.transit_accessibility(region_in, date_in, "PM")
    # acc.transit_accessibility(region_in, date_in, "WE")
    # None

def compute_tlos(region,date):

    acc.levelofservice(region,date)



# compute_tlos("New York", "2020-02-23")


compute_accessibility("New York", "2020-05-10")


# LA islands 061110036121,061119800001,060375991001,060375991002,060375990003,060375990004,060375990002,060375990001


# new york - 1 access iteration (1 period) takes a bit more than 30 min
