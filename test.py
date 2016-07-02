import csv
import collections

f1 = file('abhinav.csv', 'r')
f2 = file('search_query.txt', 'r')
f3 = file('results.csv', 'w')

c1 = csv.reader(f1)
c2 = csv.reader(f2)
c3 = csv.writer(f3)

input = [row for row in c2]

for Product_Category in c1:
    row = 1
    found = False
    for input_row in input:
        results_row = []
        if input_row[0] in booklist_row[0]:
            results_row.append('Matching')
            found = True
            break
        row = row + 1
    if not found:
        results_row.append('')
    c3.writerow(results_row)

f1.close()
f2.close()
f3.close()

d = collections.defaultdict(int)
with open("results.csv", "rb") as info:
    reader = csv.reader(info)
    for row in reader:
        for matches in row:
            matches = matches.strip()
            if matches:
                d[matches] += 1
    results = [(matches, count) for matches, count in d.iteritems() if count >= 1]
    results.sort(key=lambda x: x[1], reverse=True)
    for matches, count in results:
        print 'There are', count, 'matching results'+'.'