import json
import matplotlib.pyplot as plt

# Load read times from the file
with open('read_times.json', 'r') as f:
    read_times = json.load(f)

# Plotting the histogram
plt.hist(read_times, bins=100, range=(0, max(read_times)))
plt.xlabel('Time taken for read (sec)')
plt.ylabel('Count')
plt.title('Distribution of Read Request Times')
plt.show()
