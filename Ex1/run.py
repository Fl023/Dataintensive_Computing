from main import ChiSquaredCalculator
from sub import CategoryCounter
from collections import defaultdict
import time


if __name__ == "__main__":
    start_time = time.time()
    job1 = CategoryCounter()
    job2 = ChiSquaredCalculator()
    job2.FILES = ["./stopwords.txt", "./category_counts.txt"]

    with job1.make_runner() as runner:
        runner.run()

        category_counts = {}
        for key, value in job1.parse_output(runner.cat_output()):
            category_counts[key] = value

        with open("category_counts.txt", "w") as file:
            for key in category_counts:
                file.write(str(key) + " " + str(category_counts[key]) + "\n")

    job1_runtime = time.time() - start_time

    output_file = "output.txt"
    with job2.make_runner() as runner:
        runner.run()
        all_words = []
        my_dict = defaultdict(lambda: "")
        with open(output_file, "w") as output:
            for key, value in job2.parse_output(runner.cat_output()):
                term, chi = value
                all_words.append(term)
                my_dict[key] += " " + term + ":" + str(chi)
            for key in my_dict:
                output.write("<" + key + ">" + my_dict[key] + "\n")
            output.write(" ".join(sorted(set(all_words))) + "\n")

    job2_runtime = time.time() - (start_time + job1_runtime)

    print("Job 1 Runtime:", job1_runtime, "seconds")
    print("Job 2 Runtime:", job2_runtime, "seconds")