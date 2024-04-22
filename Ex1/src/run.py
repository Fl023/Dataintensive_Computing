from main import ChiSquaredCalculator
from sub import CategoryCounter
from collections import defaultdict
import time

'''
This file runs the two map-reduce jobs from the files "main.py" and "sub.py".
It will also print the runtimes of the two jobs and format the output of the second job,
so it will follow the specifications from the task.
'''
if __name__ == "__main__":
    start_time = time.time()  # timer is started
    # 1. job counts number of reviews per category; information will be written to "category_counts.txt"
    job1 = CategoryCounter()
    # 2. job calculates chi squared values and outputs the top 75 terms
    job2 = ChiSquaredCalculator()
    # 2. job needs two files: stopwords.txt and the output from job1 "category_counts.txt"
    job2.FILES = ["./stopwords.txt", "./category_counts.txt"]

    # running the first job
    with job1.make_runner() as runner:
        runner.run()
        category_counts = {}

        # retrieving output from job1 and storing in dictionary
        for key, value in job1.parse_output(runner.cat_output()):
            category_counts[key] = value

        # writing information to file "category_counts.txt"
        with open("category_counts.txt", "w") as file:
            for key in category_counts:
                file.write(str(key) + " " + str(category_counts[key]) + "\n")

    job1_runtime = time.time() - start_time  # timing the 1. job

    # running the second job
    output_file = "output.txt"
    with job2.make_runner() as runner:
        runner.run()
        all_words = []
        my_dict = defaultdict(lambda: "")

        with open(output_file, "w") as output:

            # retrieving output from job2 and storing in dictionary
            for key, value in job2.parse_output(runner.cat_output()):
                term, chi = value
                all_words.append(term)
                my_dict[key] += " " + term + ":" + str(chi)

            # writing chi-square values to file "output.txt"
            for key in my_dict:
                output.write("<" + key + ">" + my_dict[key] + "\n")

            # writing merged dictionary to file "output.txt"
            output.write(" ".join(sorted(set(all_words))) + "\n")

    job2_runtime = time.time() - (start_time + job1_runtime)  # timing the 2. job

    # printing runtimes to console
    print("Job 1 Runtime:", job1_runtime, "seconds")
    print("Job 2 Runtime:", job2_runtime, "seconds")
