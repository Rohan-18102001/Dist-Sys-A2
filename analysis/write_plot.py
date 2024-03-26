import json
import matplotlib.pyplot as plt

# Load write times from the file
with open('write_times.json', 'r') as f:
    write_times = json.load(f)

# Plotting the histogram
plt.hist(write_times, bins=100, range=(0, max(write_times)))
plt.xlabel('Time taken for write (sec)')
plt.ylabel('Count')
plt.title('Distribution of Write Request Times')
plt.show()
