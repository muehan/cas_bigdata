import subprocess

first = True
with open('D:\downloads\position.csv') as f:
    for line in f:
        with open('D:\downloads\position_2.csv', 'a') as the_file:
            if first:
                the_file.write(line[2:])
            else:
                the_file.write(line)