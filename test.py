import time
import math
import json
import os

# for i in range (5):
#     print(i)


# list_empty = []
# print(list_empty)
# if (list_empty == []):
#     print("kosong")

# list_empty.append(1)
# print(list_empty)
# if (list_empty != []):
#     print("TIDAK kosong")

x = 101
y = 20
z = math.ceil(x/y)

for i in range (z):
    print(i+1, end=" ")
    start = ((i+1-1) * y) + 1
    end = (i+1) * y
    print("start: ", start)
    print("end: ", end)
    

# for i in range (0,20,2):
#     print(i)

thisdict =	{
  "branddd asd": "Ford",
  "model": "Mustang",
  "year": 1964
}
print(thisdict)

# Specify the custom file path
file_path = "./export/export.json"

# Ensure the directory exists
os.makedirs(os.path.dirname(file_path), exist_ok=True)

with open("./export/export.json", "w") as json_file:
    json.dump(thisdict, json_file, indent=4)