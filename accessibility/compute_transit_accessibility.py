import acc
from multiprocessing import Pool

# python3 accessibility/compute_transit_accessibility.py

# simple runs
# acc.levelofservice("District of Columbia", "2020-06-21")
# acc.levelofservice("San Francisco-Oakland", "2021-01-17")
# acc.transit_accessibility("Philadelphia", "2021-02-14", "MP")
# acc.transit_accessibility("Philadelphia", "2021-02-14", "PM")
# acc.transit_accessibility("Philadelphia", "2021-02-14", "WE")




# running for all dates in a region:

# region = "District of Columbia"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-10","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")
#
# region = "New York"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-17","2020-06-21","2020-07-19","2020-08-16","2z020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")

# region = "Philadelphia"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-17","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")

# region = "Boston"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-10","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")

# region = "Los Angeles"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-10","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")

# region = "Chicago"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-10","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-15","2020-12-20","2021-01-17","2021-02-21")

# region = "San Francisco-Oakland"
# dates = ("2020-02-23","2020-03-15","2020-04-19","2020-05-10","2020-06-21","2020-07-19","2020-08-16","2020-09-20","2020-10-18","2020-11-22","2020-12-20","2021-01-10","2021-02-21")

# def compute_accessibility(date):
#     print(region, date)
#     acc.transit_accessibility(region, date, "MP")
#     acc.transit_accessibility(region, date, "PM")
#     acc.transit_accessibility(region, date, "WE")
#
# def compute_tlos(date):
#     print(region, date)
#     acc.levelofservice(region,date)
#
# def compute_both(date):
#     print(region, date)
#     compute_accessibility(date)
#     compute_tlos(date)
#
# if __name__ == '__main__':
#     with Pool(13) as p:
#         # pick either access, tlos, or both
#         p.map(compute_tlos, dates)
