import acc

# simple script for computing all auto accessibility measures


# region_in = "San Francisco-Oakland"
region_in = "Los Angeles"

print("meow")

print(region_in)

acc.auto_accessibility(region_in, "PM")
acc.auto_accessibility_join_single(region_in, "PM")
acc.auto_accessibility(region_in, "WE")
acc.auto_accessibility(region_in, "AM")
acc.auto_accessibility_join(region_in)


# acc.auto_accessibility_matrix_test(region_in,"AM")
# acc.auto_accessibility_matrix_test(region_in,"AM")
# acc.auto_accessibility_matrix_test(region_in,"WE")


# mkdir accessibility_chunks_AM accessibility_chunks_PM accessibility_chunks_WE
