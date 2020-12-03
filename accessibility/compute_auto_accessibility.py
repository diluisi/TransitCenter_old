import acc

# simple script for computing all auto accessibility measures

region_in = "Boston"

print("meow")
# acc.auto_accessibility(region_in, "PM")
acc.auto_accessibility(region_in, "WE")
acc.auto_accessibility(region_in, "AM")
acc.auto_accessibility_join(region_in)

print("meow")



# acc.auto_accessibility_join(region_in, "PM")

# acc.auto_accessibility(region_in, "wkd")
# acc.auto_accessibility_join(region_in, "wkd")

# acc.auto_accessibility(region_in, "8am")
# acc.auto_accessibility_join(region_in, "8am")
