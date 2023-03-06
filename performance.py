f = open("data", "r");

# binaryPerformance = {
#     "few_insert_delete":    32,
#     "few_read_dup":         51,
#     "few_read_update":      46,
#     "few_read":             45,
#     "many_insert_delete":   28,
#     "many_read_dup":        22,
#     "many_read_update":     33,
#     "many_read":            13,
#     "single_insert_delete": 40,
#     "single_read_dup":      67,
#     "single_read_update":   49,
#     "single_read":          47,
#     "test":                 2.47
# }

# currentSet = "";

ifLast = True;
lastTime = 0.0;

ratioArr = [];

for line in f:
    # print(line);
    line = line.rstrip("\n");
    items = line.split(" ");
    # print(items);
    # if (items[0] in binaryPerformance):
    #     currentSet = items[0];
    # else:
    items= line.split('\t');
    # print(items);
    if (items[0] == "real"):
        t = items[1];
        minutes = t[0];
        t = t.strip(minutes);
        minutes = int(minutes);
        t = t.strip("m");
        t = t.rstrip("s");
        realTime = float(t) + 60 * minutes;
        # print(realTime);
        if (ifLast):
            ifLast = False;
            lastTime = realTime;
        else:
            ifLast = True;
            ratio = realTime / lastTime;
            ratioArr.append(ratio);
        # print(ratio);

print(ratioArr)

import math
import numpy as np

logRatioArr = [];
for ratio in ratioArr:
    logRatioArr.append(math.log2(ratio));
C = 40 * max(0, np.average(logRatioArr)) / (0.4 + 0.1 * np.std(logRatioArr));
print("C = ", C);